"""Image service for downloading and uploading to R2"""

import logging
import os
import subprocess
import time
import threading
from pathlib import Path
from typing import Optional

import boto3
import requests
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import settings
from config.database import DatabaseManager

logger = logging.getLogger(__name__)

# VPN-related curl exit codes
VPN_ERROR_CODES = {35, 45, 55, 56}  # Interface/network errors indicating VPN down


class ImageService:
    """Service for handling product images and videos"""

    _vpn_lock = threading.Lock()
    _vpn_last_restart = 0
    _vpn_restart_cooldown = 30
    _vpn_interfaces = ["tun0", "tun1"]
    _vpn_counter = 0
    _vpn_counter_lock = threading.Lock()

    def __init__(self):
        """Initialize R2 client"""
        config = Config(max_pool_connections=50)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=settings.R2_ENDPOINT_URL,
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=config,
        )
        self.bucket_name = settings.R2_BUCKET_NAME
        self.global_vpn_enabled = getattr(settings, "USE_VPN", False)

    @classmethod
    def _is_vpn_up(cls, interface: str = None) -> bool:
        """Check if VPN interface exists and is up"""
        try:
            interfaces = [interface] if interface else ["tun0", "tun1"]
            for iface in interfaces:
                result = subprocess.run(
                    ["ip", "link", "show", iface],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0 and "UP" in result.stdout:
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking VPN status: {e}")
            return False

    def _get_vpn_interface(self) -> Optional[str]:
        """Get the active VPN interface name"""
        for interface in ["tun0", "tun1"]:
            try:
                result = subprocess.run(
                    ["ip", "link", "show", interface],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0 and "UP" in result.stdout:
                    return interface
            except Exception:
                continue
        return None

    @classmethod
    def _restart_vpn(cls, interface: str = "tun0") -> bool:
        """Restart VPN connection with cooldown to prevent rapid restarts"""
        current_time = time.time()

        # Map interface to config file
        vpn_configs = {
            "tun0": "/etc/openvpn/client/ro-free-5.protonvpn.udp.conf",
            "tun1": "/etc/openvpn/client/ro-free-9.protonvpn.udp.conf",
        }

        with cls._vpn_lock:
            if current_time - cls._vpn_last_restart < cls._vpn_restart_cooldown:
                logger.info(f"VPN restart skipped (cooldown active) for {interface}")
                time.sleep(5)
                return cls._is_vpn_up(interface)

            logger.warning(f"VPN ({interface}) appears down. Attempting restart...")

            try:
                # Kill only this interface's process
                subprocess.run(
                    ["pkill", "-f", vpn_configs[interface]],
                    capture_output=True,
                    timeout=10,
                )
                time.sleep(2)

                result = subprocess.run(
                    [
                        "openvpn",
                        "--config",
                        vpn_configs[interface],
                        "--dev",
                        interface,
                        "--daemon",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )

                if result.returncode != 0:
                    logger.error(f"VPN start failed ({interface}): {result.stderr}")
                    return False

                for _ in range(10):
                    time.sleep(2)
                    if cls._is_vpn_up(interface):
                        cls._vpn_last_restart = current_time
                        logger.info(f"VPN ({interface}) restarted successfully")
                        return True

                logger.error(f"VPN ({interface}) failed to come up after restart")
                return False

            except Exception as e:
                logger.error(f"Error restarting VPN ({interface}): {e}")
                return False

    def _ensure_vpn_up(self, interface: str = None) -> bool:
        """Ensure VPN is up, restart if needed"""
        if self._is_vpn_up(interface):
            return True
        return self._restart_vpn(interface or "tun0")

    def _download_via_vpn(self, url: str, temp_path: str, timeout: int = 360) -> bool:
        """Download file using curl through VPN interface with better error handling"""
        referer = "/".join(url.split("/")[:3]) + "/"

        if not self._ensure_vpn_up():
            logger.error(f"Cannot download {url}: VPN is down and restart failed")
            return False

        interface = self._get_vpn_interface()
        if not interface:
            logger.error(f"Cannot download {url}: No VPN interface found")
            return False

        try:
            result = subprocess.run(
                [
                    "/usr/bin/curl",
                    "--interface",
                    interface,
                    "-s",
                    "-L",
                    "-w",
                    "\n%{http_code}",
                    "-H",
                    f"User-Agent: {settings.USER_AGENT}",
                    "-H",
                    f"Referer: {referer}",
                    "-o",
                    temp_path,
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            stdout_lines = result.stdout.strip().split("\n")
            http_code = stdout_lines[-1] if stdout_lines else "000"

            if result.returncode != 0:
                if result.returncode in VPN_ERROR_CODES:
                    logger.warning(
                        f"VPN error (code {result.returncode}) on {interface} downloading {url}. "
                        f"Attempting VPN restart..."
                    )
                    if self._restart_vpn(interface):
                        logger.info(f"Retrying download after VPN restart: {url}")
                        return self._download_via_vpn_single_attempt(
                            url, temp_path, referer, timeout
                        )
                    else:
                        raise Exception(
                            f"VPN down (curl code {result.returncode}) and restart failed"
                        )
                else:
                    raise Exception(
                        f"curl failed with code {result.returncode}, "
                        f"HTTP status: {http_code}, stderr: {result.stderr}"
                    )

            if http_code.startswith(("4", "5")):
                raise Exception(f"HTTP error {http_code} for {url}")

            return True

        except subprocess.TimeoutExpired:
            raise Exception(f"Download timed out after {timeout}s for {url}")
        except Exception as e:
            raise Exception(str(e))

    def _download_via_vpn_single_attempt(
        self, url: str, temp_path: str, referer: str, timeout: int
    ) -> bool:
        """Single download attempt without VPN restart logic (used for retry)"""
        interface = self._get_vpn_interface()
        if not interface:
            raise Exception("No VPN interface available")

        try:
            result = subprocess.run(
                [
                    "/usr/bin/curl",
                    "--interface",
                    interface,
                    "-s",
                    "-L",
                    "-w",
                    "\n%{http_code}",
                    "-H",
                    f"User-Agent: {settings.USER_AGENT}",
                    "-H",
                    f"Referer: {referer}",
                    "-o",
                    temp_path,
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            stdout_lines = result.stdout.strip().split("\n")
            http_code = stdout_lines[-1] if stdout_lines else "000"

            if result.returncode != 0:
                raise Exception(
                    f"curl failed with code {result.returncode}, "
                    f"HTTP status: {http_code}, stderr: {result.stderr}"
                )

            if http_code.startswith(("4", "5")):
                raise Exception(f"HTTP error {http_code}")

            return True

        except subprocess.TimeoutExpired:
            raise Exception(f"Download timed out after {timeout}s")

    def _download_via_requests(
        self, url: str, temp_path: str, timeout: int = 360
    ) -> bool:
        """Download file using requests library"""
        referer = "/".join(url.split("/")[:3]) + "/"
        headers = {
            "User-Agent": settings.USER_AGENT,
            "Referer": referer,
        }

        response = requests.get(url, timeout=timeout, stream=True, headers=headers)
        response.raise_for_status()

        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return True

    def download_file(
        self, url: str, temp_path: str, max_retries: int = 3, use_vpn: bool = False
    ) -> bool:
        """Download file from URL, using VPN if enabled globally AND per-store"""
        should_use_vpn = self.global_vpn_enabled and use_vpn

        for attempt in range(max_retries):
            try:
                if should_use_vpn:
                    return self._download_via_vpn(url, temp_path)
                else:
                    return self._download_via_requests(url, temp_path)

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Download attempt {attempt + 1} failed, retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Error downloading {url}: {e}")
                    return False

        return False

    def upload_to_r2(
        self, local_path: str, r2_key: str, max_retries: int = 3
    ) -> Optional[str]:
        """Upload file to R2 with retry"""
        for attempt in range(max_retries):
            try:
                ext = os.path.splitext(local_path)[1].lower()
                content_types = {
                    ".png": "image/png",
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".webp": "image/webp",
                    ".mp4": "video/mp4",
                }
                content_type = content_types.get(ext, "application/octet-stream")

                with open(local_path, "rb") as f:
                    self.s3_client.upload_fileobj(
                        f,
                        self.bucket_name,
                        r2_key,
                        ExtraArgs={"ContentType": content_type},
                    )

                return f"{settings.R2_PUBLIC_URL}/{r2_key}"

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Upload attempt {attempt + 1} failed, retrying: {e}"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Error uploading to R2: {str(e)}")
                    return None

    def delete_all_images(self):
        """Delete all images from R2 bucket with pagination support"""
        try:
            deleted_count = 0
            continuation_token = None

            while True:
                if continuation_token:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name, ContinuationToken=continuation_token
                    )
                else:
                    response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

                if "Contents" not in response:
                    logger.info("No images to delete")
                    break

                objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
                self.s3_client.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": objects}
                )
                deleted_count += len(objects)

                if not response.get("IsTruncated"):
                    break

                continuation_token = response.get("NextContinuationToken")

            logger.info(f"Deleted {deleted_count} images from R2")

        except Exception as e:
            logger.error(f"Error deleting images: {str(e)}")
            raise

    def compress_video(self, input_path: str, output_path: str) -> bool:
        """Compress video using FFmpeg with better error logging"""
        try:
            import ffmpeg

            (
                ffmpeg.input(input_path)
                .output(
                    output_path,
                    vcodec="libx264",
                    video_bitrate="1M",
                    preset="veryfast",
                    acodec="aac",
                )
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
            return True
        except ffmpeg.Error as e:
            stderr_output = e.stderr.decode() if e.stderr else "No stderr"
            logger.error(f"FFmpeg error compressing {input_path}: {stderr_output}")
            return False
        except Exception as e:
            logger.error(f"Error compressing video {input_path}: {e}")
            return False

    @staticmethod
    def process_images(store_type: str = None):
        """Process images with separate worker pools for VPN and non-VPN"""
        try:
            image_service = ImageService()

            # Step 1: Fetch products and release connection
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                base_query = """
                    SELECT p.id, p.store_id, p.image_url, p.source_image_url, p.product_images, 
                        s.store_type, s.use_vpn
                    FROM products p
                    JOIN stores s ON p.store_id = s.store_id
                    WHERE p.source_image_url IS NOT NULL 
                    AND p.source_image_url != ''
                    AND (
                        SUBSTRING_INDEX(p.source_image_url, '/', -1) != SUBSTRING_INDEX(p.image_url, '/', -1)
                        OR p.image_url NOT LIKE %s
                    )
                """

                if store_type:
                    base_query += " AND s.store_type = %s"
                    cursor.execute(
                        base_query, (f"{settings.R2_PUBLIC_URL}%", store_type)
                    )
                else:
                    cursor.execute(base_query, (f"{settings.R2_PUBLIC_URL}%",))

                products = cursor.fetchall()

            # Step 2: Split by VPN requirement (connection now closed)
            vpn_products = [p for p in products if p.get("use_vpn")]
            non_vpn_products = [p for p in products if not p.get("use_vpn")]

            logger.info(
                f"Processing images: {len(non_vpn_products)} non-VPN, {len(vpn_products)} VPN"
            )

            success, failed = 0, 0
            temp_dir = Path("temp_images")
            temp_dir.mkdir(exist_ok=True)

            def process_single_image(product):
                product_id = product["id"]
                source_url = product["source_image_url"]
                product_images_str = product.get("product_images")
                current_store_type = product["store_type"]
                use_vpn = product.get("use_vpn", False)

                filename = f"{product_id}_{source_url.split('/')[-1]}"
                temp_path = temp_dir / filename

                try:
                    if not image_service.download_file(
                        source_url, str(temp_path), use_vpn=use_vpn
                    ):
                        return (product_id, None, None)

                    folder = "woo" if current_store_type == "woocommerce" else "starter"
                    r2_key = f"{folder}/images/{source_url.split('/')[-1]}"
                    r2_url = image_service.upload_to_r2(str(temp_path), r2_key)

                    if temp_path.exists():
                        os.remove(temp_path)

                    if not r2_url:
                        return (product_id, None, None)

                    # Process additional images
                    processed_images = None
                    if product_images_str:
                        image_urls = [
                            url.strip()
                            for url in product_images_str.split(",")
                            if url.strip()
                        ]
                        processed_urls = []
                        for img_url in image_urls:
                            img_filename = f"{product_id}_{img_url.split('/')[-1]}"
                            img_temp_path = temp_dir / img_filename
                            if image_service.download_file(
                                img_url, str(img_temp_path), use_vpn=use_vpn
                            ):
                                img_r2_key = f"{folder}/images/{img_url.split('/')[-1]}"
                                img_r2_url = image_service.upload_to_r2(
                                    str(img_temp_path), img_r2_key
                                )
                                if img_r2_url:
                                    processed_urls.append(img_r2_url)
                            if img_temp_path.exists():
                                os.remove(img_temp_path)
                        processed_images = (
                            ", ".join(processed_urls) if processed_urls else None
                        )

                    return (product_id, r2_url, processed_images)

                except Exception as e:
                    logger.error(f"Error processing product {product_id}: {e}")
                    if temp_path.exists():
                        os.remove(temp_path)
                    return (product_id, None, None)

            def process_batch(product_list, max_workers, label):
                nonlocal success, failed
                if not product_list:
                    return

                logger.info(f"Starting {label} batch with {max_workers} workers...")

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(process_single_image, p): p
                        for p in product_list
                    }

                    for future in as_completed(futures):
                        product_id, r2_url, product_images = future.result()

                        if r2_url:
                            # New connection for each UPDATE
                            with DatabaseManager.get_connection() as conn:
                                cursor = conn.cursor(dictionary=True)
                                cursor.execute(
                                    """UPDATE products SET image_url = %s, product_images = %s, updated_at = updated_at WHERE id = %s""",
                                    (r2_url, product_images, product_id),
                                )
                            success += 1
                            if success % 100 == 0:
                                logger.info(f"Processed {success} product images...")
                        else:
                            failed += 1

            # Process non-VPN first (fast), then VPN (throttled)
            process_batch(non_vpn_products, max_workers=20, label="non-VPN")
            process_batch(vpn_products, max_workers=10, label="VPN")

            logger.info(
                f"Image processing complete: {success} success, {failed} failed"
            )

        except Exception as e:
            logger.error(f"Error in process_images: {str(e)}")
            raise

    @staticmethod
    def process_videos(store_type: str = None):
        """Process videos with separate worker pools for VPN and non-VPN"""
        try:
            image_service = ImageService()

            # Step 1: Fetch products and release connection
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                base_query = """
                    SELECT p.id, p.video_url, p.description, s.store_type, s.use_vpn
                    FROM products p
                    JOIN stores s ON p.store_id = s.store_id
                    WHERE p.video_url IS NOT NULL 
                    AND p.video_url != ''
                    AND p.video_url NOT LIKE %s
                """

                if store_type:
                    base_query += " AND s.store_type = %s"
                    cursor.execute(
                        base_query, (f"{settings.R2_PUBLIC_URL}%", store_type)
                    )
                else:
                    cursor.execute(base_query, (f"{settings.R2_PUBLIC_URL}%",))

                products = cursor.fetchall()

            # Step 2: Split by VPN requirement (connection now closed)
            vpn_products = [p for p in products if p.get("use_vpn")]
            non_vpn_products = [p for p in products if not p.get("use_vpn")]

            logger.info(
                f"Processing videos: {len(non_vpn_products)} non-VPN, {len(vpn_products)} VPN"
            )

            success, failed = 0, 0
            temp_dir = Path("temp_videos")
            temp_dir.mkdir(exist_ok=True)

            def process_single_video(product):
                product_id = product["id"]
                video_url = product["video_url"]
                description = product.get("description")
                current_store_type = product["store_type"]
                use_vpn = product.get("use_vpn", False)

                video_filename = f"{product_id}_{video_url.split('/')[-1]}"
                video_temp_path = temp_dir / video_filename
                compressed_path = temp_dir / f"compressed_{video_filename}"

                try:
                    if not image_service.download_file(
                        video_url, str(video_temp_path), use_vpn=use_vpn
                    ):
                        return (product_id, None, None, None)

                    # Validate downloaded video
                    if not image_service._is_valid_video(str(video_temp_path)):
                        if video_temp_path.exists():
                            os.remove(video_temp_path)
                        return (product_id, None, None, None)

                    folder = "woo" if current_store_type == "woocommerce" else "starter"

                    if image_service.compress_video(
                        str(video_temp_path), str(compressed_path)
                    ):
                        video_r2_key = f"{folder}/videos/{video_url.split('/')[-1]}"
                        r2_video_url = image_service.upload_to_r2(
                            str(compressed_path), video_r2_key
                        )
                        if compressed_path.exists():
                            os.remove(compressed_path)
                    else:
                        r2_video_url = None

                    if video_temp_path.exists():
                        os.remove(video_temp_path)

                    return (product_id, r2_video_url, video_url, description)

                except Exception as e:
                    logger.error(
                        f"Error processing video for product {product_id}: {e}"
                    )
                    if video_temp_path.exists():
                        os.remove(video_temp_path)
                    if compressed_path.exists():
                        os.remove(compressed_path)
                    return (product_id, None, None, None)

            def process_batch(product_list, max_workers, label):
                nonlocal success, failed
                if not product_list:
                    return

                logger.info(
                    f"Starting {label} video batch with {max_workers} workers..."
                )

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(process_single_video, p): p
                        for p in product_list
                    }

                    for future in as_completed(futures):
                        (
                            product_id,
                            r2_video_url,
                            original_video_url,
                            description,
                        ) = future.result()

                        if r2_video_url:
                            updated_description = None
                            if (
                                original_video_url
                                and description
                                and original_video_url in description
                            ):
                                updated_description = description.replace(
                                    original_video_url, r2_video_url
                                )

                            with DatabaseManager.get_connection() as conn:
                                cursor = conn.cursor(dictionary=True)
                                if updated_description:
                                    cursor.execute(
                                        """UPDATE products SET video_url = %s, description = %s, updated_at = updated_at WHERE id = %s""",
                                        (r2_video_url, updated_description, product_id),
                                    )
                                else:
                                    cursor.execute(
                                        """UPDATE products SET video_url = %s, updated_at = updated_at WHERE id = %s""",
                                        (r2_video_url, product_id),
                                    )
                            success += 1
                            if success % 10 == 0:
                                logger.info(f"Processed {success} product videos...")
                        else:
                            # Set video_url and description to NULL on failure
                            with DatabaseManager.get_connection() as conn:
                                cursor = conn.cursor(dictionary=True)
                                cursor.execute(
                                    """UPDATE products SET video_url = NULL, description = NULL, updated_at = updated_at WHERE id = %s""",
                                    (product_id,),
                                )
                            failed += 1

            # Process non-VPN first (fast), then VPN (throttled)
            process_batch(non_vpn_products, max_workers=12, label="non-VPN")
            process_batch(vpn_products, max_workers=8, label="VPN")

            logger.info(
                f"Video processing complete: {success} success, {failed} failed"
            )

        except Exception as e:
            logger.error(f"Error in process_videos: {str(e)}")
            raise

    def _is_valid_video(self, file_path: str, min_size_kb: int = 10) -> bool:
        """Check if downloaded video file is valid"""
        try:
            # Check file exists and has minimum size
            if not os.path.exists(file_path):
                return False

            file_size = os.path.getsize(file_path)
            if file_size < min_size_kb * 1024:
                logger.warning(f"Video file too small ({file_size} bytes): {file_path}")
                return False

            # Use ffprobe to verify file is readable
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    file_path,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode != 0:
                logger.warning(f"Invalid video file (ffprobe failed): {file_path}")
                return False

            return True

        except Exception as e:
            logger.warning(f"Error validating video {file_path}: {e}")
            return False
