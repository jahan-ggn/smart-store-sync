"""Image service for downloading and uploading to R2"""

import logging
import os
import requests
import boto3
from pathlib import Path
from typing import Optional
from config.settings import settings
from config.database import DatabaseManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from botocore.config import Config


logger = logging.getLogger(__name__)


class ImageService:
    """Service for handling product images"""

    def __init__(self):
        """Initialize R2 client"""
        # Configure for parallel uploads
        config = Config(max_pool_connections=30)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=settings.R2_ENDPOINT_URL,
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=config,
        )
        self.bucket_name = settings.R2_BUCKET_NAME

    def download_image(self, image_url: str, temp_path: str) -> bool:
        """
        Download image from URL

        Args:
            image_url: URL of the image
            temp_path: Local path to save image

        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.get(image_url, timeout=30, stream=True)
            response.raise_for_status()

            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return True
        except Exception as e:
            logger.error(f"Error downloading image {image_url}: {str(e)}")
            return False

    def upload_to_r2(self, local_path: str, r2_key: str) -> Optional[str]:
        """
        Upload image to R2

        Args:
            local_path: Path to local image file
            r2_key: Key (path) in R2 bucket

        Returns:
            Public URL of uploaded image, or None if failed
        """
        try:
            # Determine content type based on extension
            ext = os.path.splitext(local_path)[1].lower()
            content_type = "image/png" if ext == ".png" else "image/jpeg"
            with open(local_path, "rb") as f:
                self.s3_client.upload_fileobj(
                    f, self.bucket_name, r2_key, ExtraArgs={"ContentType": content_type}
                )

            # Construct public URL
            public_url = f"{settings.R2_PUBLIC_URL}/{r2_key}"
            return public_url

        except Exception as e:
            logger.error(f"Error uploading to R2: {str(e)}")
            return None

    @staticmethod
    def process_all_products():
        """Process images for all products with original URLs using multi-threading"""
        try:
            image_service = ImageService()

            # Get stores that need transparent images (pro plan)
            # stores_needing_transparent = image_service._get_stores_needing_transparent()
            # logger.info(
            #     f"{len(stores_needing_transparent)} stores need transparent images"
            # )

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get products with non-R2 image URLs
                cursor.execute(
                    """SELECT id, store_id, image_url, source_image_url, product_images 
                        FROM products 
                        WHERE image_url IS NOT NULL 
                        AND image_url != '' 
                        AND source_image_url IS NOT NULL
                        AND (
                            SUBSTRING_INDEX(source_image_url, '/', -1) != SUBSTRING_INDEX(image_url, '/', -1)
                            OR image_url NOT LIKE %s
                        )""",
                    (f"{settings.R2_PUBLIC_URL}%",),
                )
                products = cursor.fetchall()

                logger.info(f"Processing images for {len(products)} products")

                success = 0
                failed = 0

                def process_single_product(product):
                    """Process a single product's image"""
                    product_id = product["id"]
                    store_id = product["store_id"]
                    source_url = product["source_image_url"]
                    product_images_str = product.get("product_images")

                    filename = f"{product_id}_{source_url.split('/')[-1]}"
                    needs_transparent = False

                    try:
                        # Download original image
                        temp_dir = Path("temp_images")
                        temp_dir.mkdir(exist_ok=True)
                        temp_path = temp_dir / filename

                        try:
                            if not image_service.download_image(
                                source_url, str(temp_path)
                            ):
                                logger.error(
                                    f"Failed to download image for product ID {product_id}: {source_url} for store ID {store_id}"
                                )
                                return (product_id, None, None, None)
                        except Exception as e:
                            logger.error(
                                f"Error downloading image for product ID {product_id} from {source_url} for store ID {store_id}: {e}"
                            )
                            return (product_id, None, None, None)

                        # Upload to R2
                        original_filename = source_url.split("/")[-1]
                        starter_key = f"starter/{original_filename}"
                        starter_url = image_service.upload_to_r2(
                            str(temp_path), starter_key
                        )

                        if not starter_url:
                            if temp_path.exists():
                                os.remove(temp_path)
                            return (product_id, None, None, None)

                        transparent_url = None

                        # If pro plan subscribers need this store, process transparent
                        # if needs_transparent:
                        #     transparent_path = image_service.remove_background(
                        #         str(temp_path)
                        #     )

                        #     if transparent_path:
                        #         # Upload to transparent folder (use PNG extension)
                        #         transparent_filename = (
                        #             f"{os.path.splitext(filename)[0]}.png"
                        #         )
                        #         transparent_key = f"transparent/{transparent_filename}"
                        #         transparent_url = image_service.upload_to_r2(
                        #             transparent_path, transparent_key
                        #         )

                        #         # Cleanup transparent file
                        #         if os.path.exists(transparent_path):
                        #             os.remove(transparent_path)

                        # Cleanup original download
                        if temp_path.exists():
                            os.remove(temp_path)

                        # Process additional images
                        processed_product_images = None
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

                                try:
                                    if not image_service.download_image(
                                        img_url, str(img_temp_path)
                                    ):
                                        logger.warning(
                                            f"Failed to download additional image for product ID {product_id} for store ID {store_id}: {img_url}"
                                        )
                                        continue

                                    img_key = f"starter/{img_url.split('/')[-1]}"
                                    img_r2_url = image_service.upload_to_r2(
                                        str(img_temp_path), img_key
                                    )

                                    if img_r2_url:
                                        processed_urls.append(img_r2_url)
                                    else:
                                        logger.warning(
                                            f"Failed to upload additional image to R2 for product ID {product_id} for store ID {store_id}: {img_filename}"
                                        )
                                except Exception as e:
                                    logger.error(
                                        f"Error processing additional image for product ID {product_id} for store ID {store_id} from {img_url}: {e}"
                                    )

                                if img_temp_path.exists():
                                    os.remove(img_temp_path)

                            processed_product_images = (
                                ", ".join(processed_urls) if processed_urls else None
                            )

                        # Return all URLs
                        return (
                            product_id,
                            starter_url,
                            transparent_url,
                            processed_product_images,
                        )

                    except Exception as e:
                        logger.error(f"Error processing product {product_id}: {str(e)}")
                        return (product_id, None, None, None)

                # Process in parallel with 10 workers
                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = {
                        executor.submit(process_single_product, p): p for p in products
                    }

                    for future in as_completed(futures):
                        (
                            product_id,
                            starter_url,
                            transparent_url,
                            product_images_urls,
                        ) = future.result()

                        if starter_url:
                            # Update database with starter URL
                            cursor.execute(
                                "UPDATE products SET image_url = %s, image_url_transparent = %s, product_images = %s, updated_at = updated_at WHERE id = %s",
                                (
                                    starter_url,
                                    transparent_url,
                                    product_images_urls,
                                    product_id,
                                ),
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
            logger.error(f"Error in process_all_products: {str(e)}")
            raise

    def delete_all_images(self):
        """Delete all images from R2 bucket with pagination support"""
        try:
            deleted_count = 0
            continuation_token = None

            while True:
                # List objects with pagination
                if continuation_token:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name, ContinuationToken=continuation_token
                    )
                else:
                    response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

                if "Contents" not in response:
                    logger.info("No images to delete")
                    break

                # Delete current batch
                objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
                self.s3_client.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": objects}
                )
                deleted_count += len(objects)

                # Check if more pages exist
                if not response.get("IsTruncated"):
                    break

                continuation_token = response.get("NextContinuationToken")

            logger.info(f"Deleted {deleted_count} images from R2")

        except Exception as e:
            logger.error(f"Error deleting images: {str(e)}")
            raise

    @staticmethod
    def _get_stores_needing_transparent() -> set:
        """Get set of store_ids that have pro plan subscribers"""
        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                cursor.execute(
                    """
                    SELECT DISTINCT sp.store_id 
                    FROM subscription_permissions sp
                    JOIN api_subscriptions sub ON sp.subscription_id = sub.id
                    WHERE sub.plan_name = 'pro' 
                    AND sub.status = 'active'
                    AND sub.expires_at > NOW()
                """
                )

                return {row["store_id"] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting stores needing transparent: {e}")
            return set()

    def remove_background(self, image_path: str, max_retries: int = 3) -> Optional[str]:
        """Remove background from image using Dezgo API with retry logic"""
        url = "https://api.dezgo.com/remove-background"

        for attempt in range(max_retries):
            try:
                with open(image_path, "rb") as f:
                    files = {"image": f}
                    headers = {"X-Dezgo-Key": settings.DEZGO_API_KEY}

                    response = requests.post(
                        url, files=files, headers=headers, timeout=120
                    )
                    response.raise_for_status()

                # Save as PNG (supports transparency)
                transparent_path = f"{os.path.splitext(image_path)[0]}.png"
                with open(transparent_path, "wb") as f:
                    f.write(response.content)

                return transparent_path

            except requests.Timeout:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Dezgo timeout (attempt {attempt + 1}/{max_retries}), retrying..."
                    )
                    time.sleep(5)
                else:
                    logger.error(f"Dezgo failed after {max_retries} attempts: Timeout")
                    return None
            except Exception as e:
                logger.error(f"Error removing background: {e}")
                return None
