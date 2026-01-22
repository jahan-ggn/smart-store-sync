"""CSV generation service for subscription data"""

import csv
import os
import logging
import shutil
import time
import re
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.database import DatabaseManager
from config.settings import settings

logger = logging.getLogger(__name__)


class CSVService:
    """Service for generating CSV files for subscriptions"""

    BASE_CSV_DIR = "csv_exports"

    @staticmethod
    def generate_csv_for_subscription(subscription_id: int) -> Optional[str]:
        """Generate CSV file for a subscription"""
        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get subscription details
                cursor.execute(
                    """SELECT buyer_domain, last_push_at 
                    FROM api_subscriptions 
                    WHERE id = %s AND expires_at > NOW()""",
                    (subscription_id,),
                )
                subscription = cursor.fetchone()

                if not subscription:
                    logger.warning(f"No active subscription found: {subscription_id}")
                    return None

                buyer_domain = subscription["buyer_domain"]
                last_push_at = subscription["last_push_at"]
                is_full_load = last_push_at is None

                # Get selected stores for this subscription
                cursor.execute(
                    """SELECT store_id FROM subscription_permissions 
                    WHERE subscription_id = %s""",
                    (subscription_id,),
                )
                store_ids = [row["store_id"] for row in cursor.fetchall()]

                if not store_ids:
                    logger.warning(
                        f"No stores selected for subscription {subscription_id}"
                    )
                    return None

                placeholders = ",".join(["%s"] * len(store_ids))

                select_columns = """
                    p.id, p.store_id, p.store_name, p.external_product_id as product_id,
                    p.product_name, p.product_url, p.image_url, p.image_url_transparent, p.product_images,
                    p.current_price, p.original_price, p.stock_status, p.is_active,
                    p.last_synced_at, p.created_at, p.updated_at, p.has_variants, p.variants,
                    p.brand_id, b.brand_name, p.video_url, p.short_description, p.description, p.attributes,
                    GROUP_CONCAT(DISTINCT c.category_id ORDER BY c.category_id SEPARATOR ', ') as categories
                """

                if is_full_load:
                    query = f"""
                        SELECT {select_columns}
                        FROM products p
                        LEFT JOIN brands b ON p.brand_id = b.brand_id
                        LEFT JOIN product_categories pc ON p.id = pc.product_id
                        LEFT JOIN categories c ON pc.category_id = c.category_id
                        WHERE p.store_id IN ({placeholders})
                        AND p.image_url IS NOT NULL AND p.image_url != ''
                        GROUP BY p.id
                    """
                    cursor.execute(query, store_ids)
                else:
                    query = f"""
                        SELECT {select_columns}
                        FROM products p
                        LEFT JOIN brands b ON p.brand_id = b.brand_id
                        LEFT JOIN product_categories pc ON p.id = pc.product_id
                        LEFT JOIN categories c ON pc.category_id = c.category_id
                        WHERE p.store_id IN ({placeholders})
                        AND (p.updated_at > %s OR p.created_at > %s)
                        AND p.image_url IS NOT NULL AND p.image_url != ''
                        GROUP BY p.id
                    """
                    cursor.execute(query, store_ids + [last_push_at, last_push_at])

                products = cursor.fetchall()

                if not products:
                    logger.info(
                        f"No products to push for subscription {subscription_id}"
                    )
                    return None

                # Create CSV directory for this buyer
                domain_clean = re.sub(r"^https?://", "", buyer_domain)
                csv_dir = Path(CSVService.BASE_CSV_DIR) / domain_clean
                csv_dir.mkdir(parents=True, exist_ok=True)

                # Generate CSV filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_filename = f"subscription_{subscription_id}_{timestamp}.csv"
                csv_path = csv_dir / csv_filename

                # Write CSV
                fieldnames = [
                    "id",
                    "store_id",
                    "store_name",
                    "product_id",
                    "product_name",
                    "product_url",
                    "image_url",
                    "image_url_transparent",
                    "product_images",
                    "current_price",
                    "original_price",
                    "stock_status",
                    "is_active",
                    "last_synced_at",
                    "created_at",
                    "updated_at",
                    "has_variants",
                    "variants",
                    "brand_id",
                    "brand_name",
                    "video_url",
                    "short_description",
                    "description",
                    "attributes",
                    "categories",
                ]

                with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(products)

                logger.info(
                    f"Generated CSV for subscription {subscription_id}: "
                    f"{len(products)} products, {'full' if is_full_load else 'incremental'} load"
                )

                return str(csv_path)

        except Exception as e:
            logger.error(
                f"Error generating CSV for subscription {subscription_id}: {str(e)}"
            )
            raise

    @staticmethod
    def push_csv_in_chunks(
        csv_path: str, subscription_id: int, chunk_size: int = 300
    ) -> Dict:
        """Push CSV file in chunks to WordPress API with multi-threading"""
        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get WooCommerce credentials
                cursor.execute(
                    """SELECT consumer_key, consumer_secret 
                    FROM woocommerce_credentials 
                    WHERE subscription_id = %s""",
                    (subscription_id,),
                )
                creds = cursor.fetchone()

                if not creds:
                    raise ValueError(
                        f"No WooCommerce credentials for subscription {subscription_id}"
                    )

                # Get buyer domain
                cursor.execute(
                    "SELECT buyer_domain FROM api_subscriptions WHERE id = %s",
                    (subscription_id,),
                )
                sub = cursor.fetchone()
                buyer_domain = sub["buyer_domain"]

                # Build WordPress API URL
                api_url = (
                    f"{buyer_domain}/wp-json/product-sync/v1/products"
                    f"?consumer_key={creds['consumer_key']}"
                    f"&consumer_secret={creds['consumer_secret']}"
                )

                # Read CSV and split into chunks
                df = pd.read_csv(csv_path)
                total_rows = len(df)
                chunks = [
                    df[i : i + chunk_size] for i in range(0, total_rows, chunk_size)
                ]

                logger.info(
                    f"Splitting {total_rows} rows into {len(chunks)} chunks for {buyer_domain}"
                )

                results = {
                    "total_chunks": len(chunks),
                    "success": 0,
                    "failed": 0,
                    "details": [],
                }

                def send_chunk(chunk_index, chunk_df, max_retries=3):
                    chunk_path = None
                    for attempt in range(max_retries):
                        try:
                            chunk_path = f"{csv_path}.chunk_{chunk_index}.csv"
                            chunk_df.to_csv(chunk_path, index=False)

                            with open(chunk_path, "rb") as f:
                                files = {"file": f}
                                response = requests.post(
                                    api_url, files=files, timeout=300
                                )

                            response.raise_for_status()
                            result = response.json()

                            return {
                                "chunk": chunk_index,
                                "status": "success",
                                "result": result,
                                "attempts": attempt + 1,
                            }

                        except Exception as e:
                            if attempt < max_retries - 1:
                                wait_time = 60 * (attempt + 1)
                                logger.warning(
                                    f"Chunk {chunk_index} failed (attempt {attempt + 1}), "
                                    f"retrying in {wait_time}s: {str(e)}"
                                )
                                time.sleep(wait_time)
                            else:
                                return {
                                    "chunk": chunk_index,
                                    "status": "failed",
                                    "error": str(e),
                                    "attempts": max_retries,
                                }
                        finally:
                            if chunk_path and os.path.exists(chunk_path):
                                os.remove(chunk_path)

                # Send chunks in parallel
                with ThreadPoolExecutor(max_workers=3) as executor:
                    future_to_chunk = {
                        executor.submit(send_chunk, i, chunk): i
                        for i, chunk in enumerate(chunks)
                    }

                    for future in as_completed(future_to_chunk):
                        chunk_result = future.result()

                        if chunk_result["status"] == "success":
                            results["success"] += 1
                            logger.info(
                                f"✓ Chunk {chunk_result['chunk']} pushed successfully"
                            )
                        else:
                            results["failed"] += 1
                            logger.error(
                                f"✗ Chunk {chunk_result['chunk']} failed: {chunk_result.get('error')}"
                            )

                        results["details"].append(chunk_result)

                logger.info(
                    f"Push complete for {buyer_domain}: "
                    f"{results['success']}/{results['total_chunks']} chunks successful"
                )

                return results

        except Exception as e:
            logger.error(
                f"Error pushing CSV for subscription {subscription_id}: {str(e)}"
            )
            raise

    @staticmethod
    def cleanup_old_csvs():
        """Delete all old CSV files before new scraper run"""
        try:
            csv_base = Path(CSVService.BASE_CSV_DIR)
            if csv_base.exists():
                shutil.rmtree(csv_base)
                logger.info("Cleaned up old CSV files")
            csv_base.mkdir(exist_ok=True)
        except Exception as e:
            logger.error(f"Error cleaning up CSVs: {str(e)}")
