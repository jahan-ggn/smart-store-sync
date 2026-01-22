"""Category scraper for WooCommerce Store API"""

import html
import json
import requests
import logging
import time
from typing import List, Dict, Set, Optional
from config.settings import settings

logger = logging.getLogger(__name__)


class CategoryScraper:
    """Scrapes categories from WooCommerce Store API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.USER_AGENT})

    def extract_categories(self, store_data: Dict) -> List[Dict]:
        """Extract all categories from store's WooCommerce API"""
        store_id = store_data["store_id"]
        store_name = store_data["store_name"]
        base_url = store_data["base_url"].rstrip("/")
        api_endpoint = store_data.get("api_endpoint", "/wp-json/wc/store")

        categories = []
        page = 1
        per_page = 100

        logger.info(f"Fetching categories for store: {store_name} (ID: {store_id})")

        try:
            while True:
                url = f"{base_url}{api_endpoint}/products/categories"
                params = {"page": page, "per_page": per_page}

                response = self.session.get(
                    url, params=params, timeout=settings.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                data = response.json()

                if not data:
                    break

                for cat in data:
                    cat_name = html.unescape(cat["name"])

                    categories.append(
                        {
                            "store_id": store_id,
                            "external_category_id": str(cat["id"]),
                            "category_name": cat_name,
                            "category_slug": cat["slug"],
                        }
                    )

                if len(data) < per_page:
                    break

                page += 1
                time.sleep(settings.REQUEST_DELAY)

            logger.info(f"Extracted {len(categories)} categories from {store_name}")

        except requests.RequestException as e:
            logger.error(f"Error fetching categories for {store_name}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error for {store_name}: {str(e)}")

        return categories

    def close(self):
        """Close the requests session"""
        self.session.close()
