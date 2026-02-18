"""Product scraper for CartPE stores"""

import re
import requests
import logging
import time
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from rapidfuzz import fuzz, process
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import settings
import json

logger = logging.getLogger(__name__)


class TokenExpiredException(Exception):
    """Custom exception for expired web tokens"""

    pass


class ProductScraper:
    """Scrapes products from CartPE store API endpoints"""

    def __init__(self, known_brands: List[str] = None, product_service=None):
        self.session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=3,
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(
            pool_connections=30, pool_maxsize=30, max_retries=retry_strategy
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": settings.USER_AGENT})

        self.known_brands = known_brands or []
        self.product_service = product_service
        logger.info(f"Loaded {len(self.known_brands)} brands for matching")

    def _extract_brand_from_name(self, product_name: str) -> Optional[str]:
        """Extract and normalize brand name from product name using multi-strategy approach"""
        if not product_name or not self.known_brands:
            return None

        # Step 1: Clean the product name
        cleaned = product_name.replace("_", "")
        cleaned = re.sub(r"[^\w\s]", " ", cleaned)
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
                        score_cutoff=88,
                    )
                    if best_match:
                        return brand_map[best_match[0]]

        # Strategy 3: Substring match for longer brands (min 4 chars)
        for brand_clean, brand_original in brand_map.items():
            if len(brand_clean) >= 4 and brand_clean in cleaned_lower:
                return brand_original

        return None

    def _extract_filename(self, url: str) -> Optional[str]:
        """Extract filename from URL"""
        if not url:
            return None
        return url.split("/")[-1]

    def extract_products(
        self, store_data: Dict, category_data: Dict, orderby: str = "new"
    ) -> List[Dict]:
        """Extract all products for a category using pagination"""
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
                # Try primary endpoint first
                api_url = f"{base_url}{api_endpoint}"
                payload = {
                    "getresult": offset,
                    "web_token": web_token,
                    "category_slug": category_slug,
                    "orderby": orderby,
                }

                logger.debug(
                    f"Fetching page {page} (offset: {offset}) for {category_name}"
                )

                response = self.session.post(
                    api_url, data=payload, timeout=settings.REQUEST_TIMEOUT
                )

                # Check for token expiration
                if response.status_code == 403:
                    logger.warning(f"Token expired for {store_name}. Needs re-fetch.")
                    raise TokenExpiredException(
                        f"Token expired for store: {store_name}"
                    )

                # If primary endpoint returns empty 500, try alternative endpoint
                if response.status_code == 500 and len(response.text.strip()) == 0:
                    logger.debug(
                        f"Primary endpoint returned empty 500, trying alternative endpoint for {category_name}"
                    )

                    api_url_new = f"{base_url}/store_product_loadmore_new"
                    payload_new = {
                        "getresult": offset,
                        "self_category_slug": category_slug,
                        "orderby": orderby,
                        "web_token": web_token,
                    }

                    response = self.session.post(
                        api_url_new,
                        data=payload_new,
                        timeout=settings.REQUEST_TIMEOUT,
                    )

                    # Check token expiration on alternative endpoint
                    if response.status_code == 403:
                        logger.warning(
                            f"Token expired for {store_name}. Needs re-fetch."
                        )
                        raise TokenExpiredException(
                            f"Token expired for store: {store_name}"
                        )

                    # If both endpoints return empty 500, it's an empty category
                    if response.status_code == 500 and len(response.text.strip()) == 0:
                        logger.info(f"Empty category: {store_name} - {category_name}")
                        break

                # Raise for other HTTP errors
                response.raise_for_status()

                # Parse HTML response
                products = self._parse_products_html(
                    response.text, store_id, store_name, category_id
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
                raise
            except requests.RequestException as e:
                logger.error(
                    f"Error fetching products for category {store_name} - {category_name} "
                    f"(page {page}, status: {getattr(e.response, 'status_code', 'N/A')}): {str(e)}"
                )
                break
            except Exception as e:
                logger.error(
                    f"Unexpected error for {store_name} - {category_name} (page {page}): {str(e)}"
                )
                break

        logger.info(
            f"Total products extracted for {category_name}: {len(all_products)}"
        )

        # Fetch additional images for products
        if all_products and self.product_service:
            products_needing_images = []

            for product in all_products:
                existing_image = self.product_service.get_existing_product_image(
                    store_id, product["external_product_id"]
                )

                existing_filename = self._extract_filename(existing_image)
                new_filename = self._extract_filename(product["image_url"])

                # Fetch additional images if product is new or image changed
                if existing_image is None or existing_filename != new_filename:
                    products_needing_images.append(product)

            logger.info(
                f"Fetching additional images for {len(products_needing_images)} products "
                f"(out of {len(all_products)} total)..."
            )

            def fetch_single_product_images(product):
                product_url = product["product_url"]
                try:
                    images = self.extract_product_images(product_url)
                    return (product_url, images)
                except requests.HTTPError as e:
                    if e.response.status_code == 404:
                        return (product_url, "SKIP")
                    return (product_url, None)
                except Exception:
                    return (product_url, None)

            # Fetch images in parallel
            image_results = {}
            if products_needing_images:
                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = {
                        executor.submit(fetch_single_product_images, p): p
                        for p in products_needing_images
                    }

                    for future in as_completed(futures):
                        url, images = future.result()
                        image_results[url] = images

            # Update products with images and remove 404 products
            all_products = [
                {**p, "product_images": image_results.get(p["product_url"])}
                for p in all_products
                if image_results.get(p["product_url"]) != "SKIP"
            ]

            logger.info(f"After filtering: {len(all_products)} products remain")

        return all_products

    def _parse_products_html(
        self, html: str, store_id: int, store_name: str, category_id: int
    ) -> List[Dict]:
        """Parse HTML response to extract product details"""
        products = []

        try:
            soup = BeautifulSoup(html, "lxml")
            product_elements = soup.select("div.col-lg-4.col-md-6.col-6")

            for element in product_elements:
                try:
                    product = self._extract_product_details(
                        element, store_id, store_name, category_id
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
        self, element, store_id: int, store_name: str, category_id: int
    ) -> Optional[Dict]:
        """Extract details from a single product element"""
        try:
            # Extract product URL and name
            link = element.select_one('a[href*=".html"]')
            if not link:
                return None

            product_url = link.get("href", "").strip()

            # Skip products with malformed URLs
            if "/.html" in product_url:
                return None

            # Extract product name from h6
            h6_tag = element.select_one("h6")
            if not h6_tag:
                return None

            product_name = h6_tag.get_text(strip=True)

            # Extract brand from product name
            brand_name = self._extract_brand_from_name(product_name)

            # Extract product ID from button
            button = element.select_one("button[data-product_id]")
            if not button:
                return None

            external_product_id = button.get("data-product_id", "").strip()

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

            product_images = None

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
                    variants = json.dumps(
                        [{"name": "Size", "value": v} for v in variant_list]
                    )

            return {
                "store_id": store_id,
                "store_name": store_name,
                "category_id": category_id,
                "external_product_id": external_product_id,
                "product_name": product_name,
                "product_url": product_url,
                "image_url": image_url,
                "source_image_url": image_url,
                "image_url_transparent": None,
                "product_images": product_images,
                "current_price": current_price,
                "original_price": original_price,
                "has_variants": has_variants,
                "variants": variants,
                "stock_status": stock_status,
                "brand_name": brand_name,
            }

        except Exception as e:
            logger.warning(f"Error extracting product details: {e}")
            return None

    def extract_product_images(
        self, product_url: str, max_retries: int = 3
    ) -> Optional[str]:
        """Extract all additional product images from product detail page"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    product_url, timeout=settings.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "lxml")

                # Find all main-image slides
                image_elements = soup.select("li.main-image img.img-fluid")

                if len(image_elements) <= 1:
                    return None

                # Extract URLs, skip first one (primary image)
                image_urls = [img.get("src", "").strip() for img in image_elements[1:]]

                # Filter out empty URLs and join
                image_urls = [url for url in image_urls if url]

                return ", ".join(image_urls) if image_urls else None

            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    raise
                # Retry on 500 errors
                if e.response.status_code == 500 and attempt < max_retries - 1:
                    logger.debug(
                        f"Retry {attempt + 1} for {product_url} after 500 error"
                    )
                    time.sleep(2 * (attempt + 1))  # 2s, 4s, 6s
                    continue
                logger.warning(f"Error extracting images from {product_url}: {e}")
                return None

            except Exception as e:
                logger.warning(f"Error extracting images from {product_url}: {e}")
                return None

        return None

    def close(self):
        """Close the requests session"""
        self.session.close()
