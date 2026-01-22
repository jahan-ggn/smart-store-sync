"""Product scraper for WooCommerce Store API"""

import html
import json
import requests
import logging
import time
import re
from typing import List, Dict, Optional, Set
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from config.settings import settings

logger = logging.getLogger(__name__)


class ProductScraper:
    """Scrapes products from WooCommerce Store API"""

    def __init__(self):
        self.session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": settings.USER_AGENT})

    def extract_products(self, store_data: Dict) -> List[Dict]:
        """Extract all products from store's WooCommerce API"""
        store_id = store_data["store_id"]
        store_name = store_data["store_name"]
        base_url = store_data["base_url"].rstrip("/")
        api_endpoint = store_data.get("api_endpoint", "/wp-json/wc/store")

        products = []
        page = 1
        per_page = 100

        logger.info(f"Fetching products for store: {store_name} (ID: {store_id})")

        while True:
            try:
                url = f"{base_url}{api_endpoint}/products"
                params = {"page": page, "per_page": per_page}

                response = self.session.get(
                    url, params=params, timeout=settings.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()

                if not data:
                    break

                for prod in data:
                    product = self._parse_product(prod, store_id, store_name)
                    if product:
                        products.append(product)

                logger.info(f"Page {page}: Found {len(data)} products")

                if len(data) < per_page:
                    break

                page += 1
                time.sleep(settings.REQUEST_DELAY)

            except requests.RequestException as e:
                logger.error(f"Error fetching page {page} for {store_name}: {str(e)}")
                if "timed out" in str(e).lower():
                    page += 1
                    time.sleep(settings.REQUEST_DELAY * 2)
                    continue
                break
            except Exception as e:
                logger.error(f"Unexpected error for {store_name}: {str(e)}")
                break

        logger.info(f"Extracted {len(products)} products from {store_name}")
        return products

    def _parse_product(
        self, prod: Dict, store_id: int, store_name: str
    ) -> Optional[Dict]:
        """Parse a single product from API response"""
        try:
            # Extract primary image
            image_url = None
            product_images = []

            if prod.get("images"):
                for i, img in enumerate(prod["images"]):
                    if i == 0:
                        image_url = img.get("src")
                    else:
                        if img.get("src"):
                            product_images.append(img["src"])

            # Extract prices
            prices = prod.get("prices", {})
            current_price = self._parse_price(prices.get("price"))
            original_price = self._parse_price(prices.get("regular_price"))

            # Extract ALL categories (no brand extraction)
            categories = []
            for cat in prod.get("categories", []):
                cat_name = html.unescape(cat["name"])
                categories.append(
                    {
                        "external_category_id": str(cat["id"]),
                        "category_name": cat_name,
                        "category_slug": cat["slug"],
                    }
                )

            description = prod.get("description", "")
            video_url = self._extract_video_url(description)

            # Determine stock status
            stock_status = (
                "in_stock" if prod.get("is_in_stock", True) else "out_of_stock"
            )

            # Extract variants
            has_variants = False
            variants = None
            if prod.get("has_options") or prod.get("variations"):
                variations_list = prod.get("variations", [])
                if variations_list:
                    has_variants = True
                    variants = json.dumps(variations_list)

            return {
                "store_id": store_id,
                "store_name": store_name,
                "external_product_id": str(prod["id"]),
                "product_name": prod.get("name", ""),
                "product_url": prod.get("permalink", ""),
                "short_description": prod.get("short_description", ""),
                "description": description,
                "image_url": image_url,
                "source_image_url": image_url,
                "product_images": ", ".join(product_images) if product_images else None,
                "current_price": current_price,
                "original_price": original_price,
                "stock_status": stock_status,
                "attributes": prod.get("attributes", []),
                "has_variants": has_variants,
                "variants": variants,
                "categories": categories,
                "video_url": video_url,
            }

        except Exception as e:
            logger.warning(f"Error parsing product {prod.get('id')}: {e}")
            return None

    def _parse_price(self, price_str: Optional[str]) -> Optional[float]:
        """Parse price string to float"""
        if not price_str:
            return None
        try:
            return float(price_str)
        except (ValueError, TypeError):
            return None

    def _extract_video_url(self, description: str) -> Optional[str]:
        """Extract video URL from description HTML"""
        if not description:
            return None
        try:
            match = re.search(r'href="([^"]+\.mp4)"', description)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None

    def close(self):
        """Close the requests session"""
        self.session.close()
