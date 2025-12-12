"""Product scraper for extracting products from store API"""

import re
import requests
import logging
import time
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from rapidfuzz import fuzz, process
from config.settings import settings
from services.database_service import BrandService

logger = logging.getLogger(__name__)


class ProductScraper:
    """Scrapes products from store API endpoints"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.USER_AGENT})
        # Load brands once during initialization
        self.known_brands = BrandService.get_all_brands()
        logger.info(f"Loaded {len(self.known_brands)} brands for matching")

    def _extract_brand_from_name(self, product_name: str) -> Optional[str]:
        """
        Extract and normalize brand name from product name using multi-strategy approach

        Args:
            product_name: Full product name

        Returns:
            Matched brand name or None
        """
        if not product_name or not self.known_brands:
            return None

        # Step 1: Clean the product name
        # Remove underscores completely (they're noise)
        cleaned = product_name.replace("_", "")
        # Remove special characters except spaces
        cleaned = re.sub(r"[^\w\s]", " ", cleaned)
        # Normalize whitespace
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        cleaned_lower = cleaned.lower()

        # Step 2: Create normalized brand lookup
        brand_map = {}
        for brand in self.known_brands:
            brand_clean = re.sub(r"[^\w\s]", " ", brand)
            brand_clean = re.sub(r"\s+", " ", brand_clean).strip().lower()
            brand_map[brand_clean] = brand

        # Strategy 1: Exact word boundary match
        for brand_clean, brand_original in brand_map.items():
            # Check if brand appears as complete word(s) at start or anywhere
            pattern = r"\b" + re.escape(brand_clean) + r"\b"
            if re.search(pattern, cleaned_lower):
                return brand_original

        # Strategy 2: Fuzzy match on first few words (higher threshold)
        words = cleaned_lower.split()
        if words:
            for num_words in [3, 2, 1]:
                if len(words) >= num_words:
                    candidate = " ".join(words[:num_words])

                    best_match = process.extractOne(
                        candidate,
                        list(brand_map.keys()),
                        scorer=fuzz.ratio,
                        score_cutoff=88,  # High threshold for accuracy
                    )

                    if best_match:
                        return brand_map[best_match[0]]

        # Strategy 3: Substring match for longer brands (min 4 chars)
        for brand_clean, brand_original in brand_map.items():
            if len(brand_clean) >= 4 and brand_clean in cleaned_lower:
                return brand_original

        return None

    def extract_products(
        self, store_data: Dict, category_data: Dict, orderby: str = "new"
    ) -> List[Dict]:
        """
        Extract all products for a category using pagination

        Args:
            store_data: Dictionary containing store information
            category_data: Dictionary containing category information
            orderby: Sorting order (default: 'new')

        Returns:
            List of product dictionaries
        """
        store_id = store_data["store_id"]
        store_name = store_data["store_name"]
        base_url = store_data["base_url"].rstrip("/")
        api_endpoint = store_data["api_endpoint"]
        web_token = store_data["web_token"]

        category_id = category_data["category_id"]
        category_name = category_data["category_name"]
        category_slug = category_data["category_slug"]

        all_products = []
        offset = 0
        page = 1

        logger.info(f"Fetching products for {store_name} - {category_name}")

        while True:
            try:
                # Construct API URL
                api_url = f"{base_url}{api_endpoint}"

                # Prepare POST data
                payload = {
                    "getresult": offset,
                    "web_token": web_token,
                    "category_slug": category_slug,
                    "orderby": orderby,
                }

                logger.debug(
                    f"Fetching page {page} (offset: {offset}) for {category_name}"
                )

                # Make API request
                response = self.session.post(
                    api_url, data=payload, timeout=settings.REQUEST_TIMEOUT
                )

                # Check for token expiration
                if response.status_code == 403:
                    logger.warning(f"Token expired for {store_name}. Needs re-fetch.")
                    raise TokenExpiredException(
                        f"Token expired for store: {store_name}"
                    )

                response.raise_for_status()

                # Parse HTML response
                products = self._parse_products_html(
                    response.text, store_id, category_id
                )

                if not products:
                    logger.info(f"No more products found. Total pages: {page}")
                    break

                all_products.extend(products)
                logger.info(f"Page {page}: Found {len(products)} products")

                # Move to next page
                offset += 12
                page += 1

                # Add delay between requests
                time.sleep(settings.REQUEST_DELAY)

            except TokenExpiredException:
                raise  # Re-raise to handle at higher level
            except requests.RequestException as e:
                logger.error(f"Error fetching products (page {page}): {str(e)}")
                break
            except Exception as e:
                logger.error(f"Unexpected error (page {page}): {str(e)}")
                break

        logger.info(
            f"Total products extracted for {category_name}: {len(all_products)}"
        )
        return all_products

    def _parse_products_html(
        self, html: str, store_id: int, category_id: int
    ) -> List[Dict]:
        """
        Parse HTML response to extract product details

        Args:
            html: HTML response string
            store_id: Store ID
            category_id: Category ID

        Returns:
            List of product dictionaries
        """
        products = []

        try:
            soup = BeautifulSoup(html, "lxml")

            # Find all product containers
            product_elements = soup.select("div.col-lg-4.col-md-6.col-6")

            for element in product_elements:
                try:
                    product = self._extract_product_details(
                        element, store_id, category_id
                    )
                    if product:
                        products.append(product)
                except Exception as e:
                    logger.warning(f"Error parsing product element: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")

        return products

    def _extract_product_details(
        self, element, store_id: int, category_id: int
    ) -> Optional[Dict]:
        """
        Extract details from a single product element

        Args:
            element: BeautifulSoup element
            store_id: Store ID
            category_id: Category ID

        Returns:
            Product dictionary or None
        """
        try:
            # Extract product URL and name
            link = element.select_one('a[href*=".html"]')
            if not link:
                return None

            product_url = link.get("href", "").strip()

            # Extract product name from h6
            h6_tag = element.select_one("h6")
            if not h6_tag:
                return None

            product_name = h6_tag.get_text(strip=True)

            # Extract brand from product name
            brand_name = self._extract_brand_from_name(product_name)
            brand_id = None
            if brand_name:
                brand_id = BrandService.get_brand_id_by_name(brand_name)

            # Extract product ID from button
            button = element.select_one("button[data-product_id]")
            if not button:
                return None

            product_id = button.get("data-product_id", "").strip()

            # Determine stock status from button text
            button_text = button.get_text(strip=True)
            stock_status = (
                "out_of_stock" if button_text.lower() == "sold out" else "in_stock"
            )

            # Extract image URL
            img = element.select_one("img.img-fluid")
            image_url = img.get("src", "").strip() if img else ""

            # Skip products without images
            if not image_url:
                return None

            # Transform gallery_sm to gallery_md
            if image_url and "gallery_sm" in image_url:
                image_url = image_url.replace("gallery_sm", "gallery_md")

            # Extract prices
            price_elements = element.select("h6")[1:]
            current_price = None
            original_price = None

            for price_elem in price_elements:
                # Get text excluding child elements (like <i> icon)
                price_text = "".join(
                    price_elem.find_all(text=True, recursive=False)
                ).strip()

                # Skip if no text found
                if not price_text:
                    continue

                # Current price (without l-through class)
                if "l-through" not in price_elem.get("class", []):
                    price_match = re.search(r"[\d,]+", price_text)
                    if price_match and current_price is None:
                        current_price = float(price_match.group().replace(",", ""))

                # Original price (with l-through class)
                if "l-through" in price_elem.get("class", []):
                    price_match = re.search(r"[\d,]+", price_text)
                    if price_match:
                        original_price = float(price_match.group().replace(",", ""))

            # Extract variants (size labels)
            variant_labels = element.select("label.badge.badge-primary")
            has_variants = False
            variants = None

            if variant_labels:
                variant_list = [
                    label.get_text(strip=True)
                    for label in variant_labels
                    if label.get_text(strip=True)
                ]
                if variant_list:
                    has_variants = True
                    variants = ", ".join(variant_list)

            return {
                "store_id": store_id,
                "category_id": category_id,
                "product_id": product_id,
                "product_name": product_name,
                "product_url": product_url,
                "image_url": image_url,
                "current_price": current_price,
                "original_price": original_price,
                "has_variants": has_variants,
                "variants": variants,
                "stock_status": stock_status,
                "brand_id": brand_id,
            }

        except Exception as e:
            logger.warning(f"Error extracting product details: {e}")
            return None

    def close(self):
        """Close the requests session"""
        self.session.close()


class TokenExpiredException(Exception):
    """Custom exception for expired web tokens"""

    pass
