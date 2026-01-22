"""Web token scraper for extracting tokens from CartPE store homepages"""

import re
import requests
import logging
import time
from typing import Optional, Dict
from config.settings import settings

logger = logging.getLogger(__name__)


class TokenScraper:
    """Scrapes web tokens from CartPE store homepages"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.USER_AGENT})

    def extract_token(self, store_data: Dict) -> Optional[str]:
        """Extract web token from store homepage"""
        store_id = store_data["store_id"]
        store_name = store_data["store_name"]
        base_url = store_data["base_url"]

        try:
            logger.info(f"Fetching token for store: {store_name} (ID: {store_id})")

            response = self.session.get(base_url, timeout=settings.REQUEST_TIMEOUT)
            response.raise_for_status()

            pattern = r'var\s+web_token\s*=\s*["\']([a-f0-9]+)["\']'
            match = re.search(pattern, response.text, re.IGNORECASE)

            if match:
                token = match.group(1)
                logger.info(f"Token extracted for {store_name}: {token[:20]}...")
                return token
            else:
                logger.warning(f"Token not found for {store_name}")
                return None

        except requests.RequestException as e:
            logger.error(f"Error fetching token for {store_name}: {e}")
            return None
        finally:
            time.sleep(settings.REQUEST_DELAY)

    def close(self):
        """Close the requests session"""
        self.session.close()
