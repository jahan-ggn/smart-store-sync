"""Category scraper for CartPE stores"""

import re
import requests
import logging
import time
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from config.settings import settings

logger = logging.getLogger(__name__)


class CategoryScraper:
    """Scrapes categories from CartPE /allcategory.html pages"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.USER_AGENT})

    def extract_categories(self, store_data: Dict) -> List[Dict]:
        """Extract all categories from store's allcategory page"""
        store_id = store_data["store_id"]
        store_name = store_data["store_name"]
        base_url = store_data["base_url"].rstrip("/")

        categories = []

        try:
            url = f"{base_url}/allcategory.html"
            logger.info(f"Fetching categories for: {store_name} (ID: {store_id})")

            response = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")
            elements = soup.select("div.cat-area a")

            for element in elements:
                try:
                    category_url = element.get("href", "").strip()
                    h4_tag = element.find("h4", class_="cat-text")

                    if not h4_tag:
                        continue

                    category_name = h4_tag.get_text(strip=True)
                    category_slug = self._extract_slug(category_url)

                    if category_name and category_url and category_slug:
                        categories.append(
                            {
                                "store_id": store_id,
                                "category_name": category_name,
                                "category_slug": category_slug,
                                "category_url": category_url,
                            }
                        )

                except Exception as e:
                    logger.warning(f"Error parsing category element: {e}")
                    continue

            logger.info(f"Extracted {len(categories)} categories from {store_name}")

        except requests.RequestException as e:
            logger.error(f"Error fetching categories for {store_name}: {e}")
        finally:
            time.sleep(settings.REQUEST_DELAY)

        return categories

    def _extract_slug(self, url: str) -> Optional[str]:
        """Extract slug from category URL"""
        try:
            match = re.search(r"/([^/]+)\.html$", url)
            return match.group(1) if match else None
        except Exception:
            return None

    def close(self):
        """Close the requests session"""
        self.session.close()
