"""Database service layer for CRUD operations"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from config.database import DatabaseManager
from config.settings import settings

logger = logging.getLogger(__name__)


class StoreService:
    """Service class for store-related database operations"""

    @staticmethod
    def get_all_stores(store_type: str = None) -> List[Dict]:
        """Fetch stores from database, optionally filtered by type"""
        if store_type:
            query = "SELECT * FROM stores WHERE store_type = %s"
            params = (store_type,)
        else:
            query = "SELECT * FROM stores"
            params = None

        try:
            stores = DatabaseManager.execute_query(query, params, fetch=True)
            logger.info(f"Found {len(stores)} stores in database")
            return stores
        except Exception as e:
            logger.error(f"Error fetching stores: {e}")
            return []

    @staticmethod
    def create_store(store_data: Dict) -> Dict:
        """Create a new store"""
        query = """
            INSERT INTO stores (store_type, store_name, store_slug, base_url, api_endpoint)
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            params = (
                store_data.get("store_type", "cartpe"),
                store_data["store_name"],
                store_data["store_slug"],
                store_data["base_url"],
                store_data.get("api_endpoint"),
            )

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                store_id = cursor.lastrowid
                conn.commit()

            logger.info(f"Created store: {store_data['store_name']}")
            return {"success": True, "store_id": store_id}

        except Exception as e:
            logger.error(f"Error creating store: {e}")
            raise

    @staticmethod
    def update_store_token(store_id: int, token: str) -> bool:
        """Update store with extracted web token (CartPE only)"""
        query = """
            UPDATE stores
            SET web_token = %s, token_last_fetched_at = %s, updated_at = %s
            WHERE store_id = %s
        """
        try:
            now = datetime.now()
            params = (token, now, now, store_id)
            rows_affected = DatabaseManager.execute_query(query, params)

            if rows_affected > 0:
                logger.info(f"Token updated for store ID: {store_id}")
                return True
            else:
                logger.warning(f"No rows updated for store ID: {store_id}")
                return False

        except Exception as e:
            logger.error(f"Error updating token for store {store_id}: {e}")
            return False


class CategoryService:
    """Service class for category-related database operations"""

    @staticmethod
    def bulk_insert_categories(categories: List[Dict]) -> int:
        """Bulk insert categories using INSERT IGNORE"""
        if not categories:
            return 0

        query = """
            INSERT IGNORE INTO categories 
            (store_id, external_category_id, category_name, category_slug, category_url, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            now = datetime.now()
            data = [
                (
                    cat["store_id"],
                    cat.get("external_category_id"),
                    cat["category_name"],
                    cat["category_slug"],
                    cat.get("category_url"),
                    now,
                )
                for cat in categories
            ]

            rows_affected = DatabaseManager.execute_many(query, data)
            logger.info(f"Inserted {rows_affected} categories into database")
            return rows_affected

        except Exception as e:
            logger.error(f"Error bulk inserting categories: {e}")
            return 0

    @staticmethod
    def get_categories_by_store(store_id: int) -> List[Dict]:
        """Get all categories for a specific store"""
        query = "SELECT * FROM categories WHERE store_id = %s"
        try:
            categories = DatabaseManager.execute_query(query, (store_id,), fetch=True)
            logger.info(f"Found {len(categories)} categories for store ID: {store_id}")
            return categories
        except Exception as e:
            logger.error(f"Error fetching categories for store {store_id}: {e}")
            return []

    @staticmethod
    def get_category_id_by_external_id(
        store_id: int, external_category_id: str
    ) -> Optional[int]:
        """Get internal category_id from external category ID"""
        query = """
            SELECT category_id FROM categories 
            WHERE store_id = %s AND external_category_id = %s
        """
        try:
            result = DatabaseManager.execute_query(
                query, (store_id, external_category_id), fetch=True
            )
            return result[0]["category_id"] if result else None
        except Exception as e:
            logger.error(f"Error fetching category: {e}")
            return None

    @staticmethod
    def get_category_id_by_slug(store_id: int, category_slug: str) -> Optional[int]:
        """Get internal category_id from category slug"""
        query = """
            SELECT category_id FROM categories 
            WHERE store_id = %s AND category_slug = %s
        """
        try:
            result = DatabaseManager.execute_query(
                query, (store_id, category_slug), fetch=True
            )
            return result[0]["category_id"] if result else None
        except Exception as e:
            logger.error(f"Error fetching category by slug: {e}")
            return None


class ProductService:
    """Service class for product-related database operations"""

    @staticmethod
    def bulk_upsert_products(products: List[Dict]) -> int:
        """Bulk insert or update products"""
        if not products:
            return 0

        r2_domain = settings.R2_PUBLIC_URL.replace("https://", "").replace(
            "http://", ""
        )

        query = f"""
            INSERT INTO products 
            (store_id, store_name, external_product_id, product_name, product_url, 
            image_url, source_image_url, image_url_transparent, product_images, 
            current_price, original_price, has_variants, variants,
            stock_status, is_active, brand_id, video_url, short_description, description, attributes,
            last_synced_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_name = VALUES(product_name),
            product_url = VALUES(product_url),
            image_url = IF(image_url LIKE '%{r2_domain}%', image_url, VALUES(image_url)),
            source_image_url = VALUES(source_image_url),
            image_url_transparent = IF(image_url_transparent LIKE '%{r2_domain}%', image_url_transparent, VALUES(image_url_transparent)),
            product_images = IF(product_images LIKE '%{r2_domain}%', product_images, VALUES(product_images)),
            current_price = VALUES(current_price),
            original_price = VALUES(original_price),
            has_variants = VALUES(has_variants),
            variants = VALUES(variants),
            stock_status = VALUES(stock_status),
            is_active = VALUES(is_active),
            brand_id = VALUES(brand_id),
            video_url = VALUES(video_url),
            short_description = VALUES(short_description),
            description = VALUES(description),
            attributes = VALUES(attributes),
            last_synced_at = VALUES(last_synced_at),
            updated_at = IF(
                product_name = VALUES(product_name) AND
                current_price = VALUES(current_price) AND
                original_price = VALUES(original_price) AND
                stock_status = VALUES(stock_status) AND
                source_image_url = VALUES(source_image_url),
                updated_at,
                VALUES(updated_at)
            )
        """

        try:
            now = datetime.now()

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                for prod in products:
                    # Get brand_id
                    brand_id = prod.get("brand_id")
                    if not brand_id and prod.get("brand_name"):
                        brand_id = BrandService.get_or_create_brand(prod["brand_name"])

                    # Serialize attributes if present
                    attributes = prod.get("attributes")
                    if isinstance(attributes, list):
                        attributes = json.dumps(attributes) if attributes else None

                    data = (
                        prod["store_id"],
                        prod["store_name"],
                        prod["external_product_id"],
                        prod["product_name"],
                        prod.get("product_url"),
                        prod.get("image_url"),
                        prod.get("source_image_url"),
                        prod.get("image_url_transparent"),
                        prod.get("product_images"),
                        prod.get("current_price"),
                        prod.get("original_price"),
                        prod.get("has_variants", False),
                        prod.get("variants"),
                        prod.get("stock_status", "in_stock"),
                        True,  # is_active
                        brand_id,
                        prod.get("video_url"),
                        prod.get("short_description"),
                        prod.get("description"),
                        attributes,
                        now,  # last_synced_at
                        now,  # created_at
                        now,  # updated_at
                    )

                    cursor.execute(query, data)
                    product_id = cursor.lastrowid

                    # If update, get the existing id
                    if not product_id:
                        cursor.execute(
                            "SELECT id FROM products WHERE store_id = %s AND external_product_id = %s",
                            (prod["store_id"], prod["external_product_id"]),
                        )
                        result = cursor.fetchone()
                        product_id = result["id"] if result else None

                    # Insert categories into junction table
                    if product_id and prod.get("categories"):
                        # Clear existing categories for this product
                        cursor.execute(
                            "DELETE FROM product_categories WHERE product_id = %s",
                            (product_id,),
                        )

                        for cat in prod["categories"]:
                            cat_id = None
                            if cat.get("external_category_id"):
                                cat_id = CategoryService.get_category_id_by_external_id(
                                    prod["store_id"], cat["external_category_id"]
                                )
                            elif cat.get("category_slug"):
                                cat_id = CategoryService.get_category_id_by_slug(
                                    prod["store_id"], cat["category_slug"]
                                )

                            if cat_id:
                                cursor.execute(
                                    "INSERT IGNORE INTO product_categories (product_id, category_id) VALUES (%s, %s)",
                                    (product_id, cat_id),
                                )

                    # For CartPE (single category_id passed directly)
                    elif product_id and prod.get("category_id"):
                        cursor.execute(
                            "DELETE FROM product_categories WHERE product_id = %s",
                            (product_id,),
                        )
                        cursor.execute(
                            "INSERT IGNORE INTO product_categories (product_id, category_id) VALUES (%s, %s)",
                            (product_id, prod["category_id"]),
                        )

                conn.commit()

            logger.info(f"Upserted {len(products)} products into database")
            return len(products)

        except Exception as e:
            logger.error(f"Error bulk upserting products: {e}")
            return 0

    @staticmethod
    def mark_category_products_inactive(store_id: int, category_id: int) -> int:
        """Mark all products in a category as inactive (CartPE)"""
        query = """
            UPDATE products p
            JOIN product_categories pc ON p.id = pc.product_id
            SET p.is_active = FALSE
            WHERE p.store_id = %s AND pc.category_id = %s
        """
        try:
            rows_affected = DatabaseManager.execute_query(
                query, (store_id, category_id)
            )
            logger.info(
                f"Marked {rows_affected} products as inactive for category {category_id}"
            )
            return rows_affected
        except Exception as e:
            logger.error(f"Error marking products inactive: {e}")
            return 0

    @staticmethod
    def mark_store_products_inactive(store_id: int) -> int:
        """Mark all products for a store as inactive (WooCommerce)"""
        query = """
            UPDATE products 
            SET is_active = FALSE, updated_at = updated_at
            WHERE store_id = %s
        """
        try:
            rows_affected = DatabaseManager.execute_query(query, (store_id,))
            logger.info(
                f"Marked {rows_affected} products as inactive for store {store_id}"
            )
            return rows_affected
        except Exception as e:
            logger.error(f"Error marking products inactive: {e}")
            return 0

    @staticmethod
    def get_existing_product_image(
        store_id: int, external_product_id: str
    ) -> Optional[str]:
        """Get existing product's main image URL"""
        query = """
            SELECT image_url FROM products 
            WHERE store_id = %s AND external_product_id = %s
        """
        try:
            result = DatabaseManager.execute_query(
                query, (store_id, external_product_id), fetch=True
            )
            return result[0]["image_url"] if result else None
        except Exception as e:
            logger.error(f"Error fetching product image: {e}")
            return None

    @staticmethod
    def get_product_count_by_store(store_id: int) -> int:
        """Get total product count for a store"""
        query = "SELECT COUNT(*) as count FROM products WHERE store_id = %s"
        try:
            result = DatabaseManager.execute_query(query, (store_id,), fetch=True)
            return result[0]["count"] if result else 0
        except Exception as e:
            logger.error(f"Error getting product count: {e}")
            return 0


class BrandService:
    """Service class for brand-related database operations"""

    @staticmethod
    def get_all_brands() -> List[str]:
        """Get all brand names from database"""
        query = "SELECT brand_name FROM brands"
        try:
            results = DatabaseManager.execute_query(query, fetch=True)
            brands = [row["brand_name"] for row in results]
            logger.info(f"Loaded {len(brands)} brands from database")
            return brands
        except Exception as e:
            logger.error(f"Error fetching brands: {e}")
            return []

    @staticmethod
    def get_brand_id_by_name(brand_name: str) -> Optional[int]:
        """Get brand ID by exact brand name match"""
        query = "SELECT brand_id FROM brands WHERE brand_name = %s"
        try:
            result = DatabaseManager.execute_query(query, (brand_name,), fetch=True)
            return result[0]["brand_id"] if result else None
        except Exception as e:
            logger.error(f"Error getting brand ID: {e}")
            return None

    @staticmethod
    def get_or_create_brand(brand_name: str) -> Optional[int]:
        """Get brand ID by name, or create if not exists"""
        brand_id = BrandService.get_brand_id_by_name(brand_name)
        if brand_id:
            return brand_id

        query = "INSERT INTO brands (brand_name) VALUES (%s)"
        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (brand_name,))
                brand_id = cursor.lastrowid
                conn.commit()
            logger.info(f"Created new brand: {brand_name}")
            return brand_id
        except Exception as e:
            logger.error(f"Error creating brand: {e}")
            return None
