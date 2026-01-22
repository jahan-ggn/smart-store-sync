"""Category scraper for WooCommerce Store API"""

import html
import json
import requests
import subprocess
import logging
import time
from typing import List, Dict, Optional
from config.settings import settings

logger = logging.getLogger(__name__)


class CategoryScraper:
    """Scrapes categories from WooCommerce Store API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.USER_AGENT})
        self.use_vpn = getattr(settings, "USE_VPN", False)

    def _vpn_get(self, url: str, timeout: int = 30) -> Optional[Dict]:
        """Make GET request through VPN interface"""
        try:
            result = subprocess.run(
                ["/usr/bin/curl", "--interface", "tun0", "-s", "-f", url],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if result.returncode != 0:
                logger.error(f"VPN request failed: {result.stderr}")
                return None
            return json.loads(result.stdout)
        except Exception as e:
            logger.error(f"VPN request error: {e}")
            return None

    def _get(self, url: str, params: Dict = None) -> Optional[List]:
        """Make GET request, using VPN if enabled"""
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        if self.use_vpn:
            return self._vpn_get(url)
        else:
            response = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()

    def extract_categories(self, store_data: Dict) -> List[Dict]:
        """Extract all categories from store's WooCommerce API"""
        store_id = store_data["store_id"]
        store_name = store_data["store_name"]
        base_url = store_data["base_url"].rstrip("/")
        api_endpoint = store_data.get("api_endpoint", "/wp-json/wc/store")

        categories = []
        page = 1
        per_page = 100

        logger.info(
            f"Fetching categories for store: {store_name} (ID: {store_id}) [VPN: {self.use_vpn}]"
        )

        try:
            while True:
                url = f"{base_url}{api_endpoint}/products/categories"
                params = {"page": page, "per_page": per_page}

                data = self._get(url, params)

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
