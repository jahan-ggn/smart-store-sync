"""Image service for downloading and uploading to R2"""

import logging
import os
import requests
import boto3
import time
from pathlib import Path
from typing import Optional
from config.settings import settings
from config.database import DatabaseManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config

logger = logging.getLogger(__name__)


class ImageService:
    """Service for handling product images and videos"""

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

    def download_file(self, url: str, temp_path: str, max_retries: int = 3) -> bool:
        """Download file from URL"""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=360, stream=True)
                response.raise_for_status()
                with open(temp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Error downloading {url}: {str(e)}")
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
        """Compress video using FFmpeg"""
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
                .run(quiet=True)
            )
            return True
        except Exception as e:
            logger.error(f"Error compressing video: {e}")
            return False

    @staticmethod
    def process_images(store_type: str = None):
        """Process images only for all products needing upload"""
        try:
            image_service = ImageService()

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                base_query = """
                    SELECT p.id, p.store_id, p.image_url, p.source_image_url, p.product_images, s.store_type
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
                logger.info(f"Processing images for {len(products)} products")

                success, failed = 0, 0
                temp_dir = Path("temp_images")
                temp_dir.mkdir(exist_ok=True)

                def process_single_image(product):
                    product_id = product["id"]
                    source_url = product["source_image_url"]
                    product_images_str = product.get("product_images")
                    current_store_type = product["store_type"]

                    filename = f"{product_id}_{source_url.split('/')[-1]}"
                    temp_path = temp_dir / filename

                    try:
                        if not image_service.download_file(source_url, str(temp_path)):
                            return (product_id, None, None)

                        folder = (
                            "woo" if current_store_type == "woocommerce" else "starter"
                        )
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
                                    img_url, str(img_temp_path)
                                ):
                                    img_r2_key = (
                                        f"{folder}/images/{img_url.split('/')[-1]}"
                                    )
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

                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = {
                        executor.submit(process_single_image, p): p for p in products
                    }

                    for future in as_completed(futures):
                        product_id, r2_url, product_images = future.result()

                        if r2_url:
                            cursor.execute(
                                """UPDATE products SET image_url = %s, product_images = %s, updated_at = updated_at WHERE id = %s""",
                                (r2_url, product_images, product_id),
                            )
                            conn.commit()
                            success += 1
                            if success % 100 == 0:
                                logger.info(f"Processed {success} product images...")
                        else:
                            failed += 1

                logger.info(
                    f"Image processing complete: {success} success, {failed} failed"
                )

        except Exception as e:
            logger.error(f"Error in process_images: {str(e)}")
            raise

    @staticmethod
    def process_videos(store_type: str = None):
        """Process videos only for products with video_url"""
        try:
            image_service = ImageService()

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                base_query = """
                    SELECT p.id, p.video_url, p.description, s.store_type
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
                logger.info(f"Processing videos for {len(products)} products")

                success, failed = 0, 0
                temp_dir = Path("temp_videos")
                temp_dir.mkdir(exist_ok=True)

                def process_single_video(product):
                    product_id = product["id"]
                    video_url = product["video_url"]
                    description = product.get("description")
                    current_store_type = product["store_type"]

                    video_filename = f"{product_id}_{video_url.split('/')[-1]}"
                    video_temp_path = temp_dir / video_filename
                    compressed_path = temp_dir / f"compressed_{video_filename}"

                    try:
                        if not image_service.download_file(
                            video_url, str(video_temp_path)
                        ):
                            return (product_id, None, None, None)

                        folder = (
                            "woo" if current_store_type == "woocommerce" else "starter"
                        )

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

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(process_single_video, p): p for p in products
                    }

                    for future in as_completed(futures):
                        product_id, r2_video_url, original_video_url, description = (
                            future.result()
                        )

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
                            conn.commit()
                            success += 1
                            if success % 10 == 0:
                                logger.info(f"Processed {success} product videos...")
                        else:
                            failed += 1

                logger.info(
                    f"Video processing complete: {success} success, {failed} failed"
                )

        except Exception as e:
            logger.error(f"Error in process_videos: {str(e)}")
            raise
