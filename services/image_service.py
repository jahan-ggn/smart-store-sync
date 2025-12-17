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

logger = logging.getLogger(__name__)


class ImageService:
    """Service for handling product images"""

    def __init__(self):
        """Initialize R2 client"""
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=settings.R2_ENDPOINT_URL,
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
            region_name="auto",
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
            with open(local_path, "rb") as f:
                self.s3_client.upload_fileobj(
                    f, self.bucket_name, r2_key, ExtraArgs={"ContentType": "image/jpeg"}
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

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get products with non-R2 image URLs
                cursor.execute(
                    """SELECT id, image_url FROM products 
                    WHERE image_url IS NOT NULL 
                    AND image_url != '' 
                    AND image_url NOT LIKE %s""",
                    (f"{settings.R2_ENDPOINT_URL}%",),
                )
                products = cursor.fetchall()

                logger.info(f"Processing images for {len(products)} products")

                success = 0
                failed = 0

                def process_single_product(product):
                    """Process a single product's image"""
                    product_id = product["id"]
                    original_url = product["image_url"]

                    # Extract filename
                    filename = original_url.split("/")[-1]
                    r2_key = filename  # Flat structure - just filename

                    try:
                        # Download
                        temp_dir = Path("temp_images")
                        temp_dir.mkdir(exist_ok=True)
                        temp_path = temp_dir / filename

                        if not image_service.download_image(
                            original_url, str(temp_path)
                        ):
                            return (product_id, None)

                        # Upload to R2
                        r2_url = image_service.upload_to_r2(str(temp_path), r2_key)

                        # Cleanup
                        if temp_path.exists():
                            os.remove(temp_path)

                        return (product_id, r2_url)

                    except Exception as e:
                        logger.error(f"Error processing product {product_id}: {str(e)}")
                        return (product_id, None)

                # Process in parallel with 10 workers
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(process_single_product, p): p for p in products
                    }

                    for future in as_completed(futures):
                        product_id, r2_url = future.result()

                        if r2_url:
                            # Update database
                            cursor.execute(
                                "UPDATE products SET image_url = %s, updated_at = updated_at WHERE id = %s",
                                (r2_url, product_id),
                            )
                            conn.commit()
                            success += 1

                            if success % 100 == 0:
                                logger.info(f"Processed {success} images...")
                        else:
                            failed += 1

                logger.info(
                    f"Image processing complete: {success} success, {failed} failed"
                )

        except Exception as e:
            logger.error(f"Error in process_all_products: {str(e)}")
            raise

    def delete_all_images(self):
        """Delete all images from R2 bucket"""
        try:
            # List all objects
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

            if "Contents" not in response:
                logger.info("No images to delete")
                return

            # Delete all objects
            objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
            self.s3_client.delete_objects(
                Bucket=self.bucket_name, Delete={"Objects": objects}
            )

            logger.info(f"Deleted {len(objects)} images from R2")

        except Exception as e:
            logger.error(f"Error deleting images: {str(e)}")
            raise
