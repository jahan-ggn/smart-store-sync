"""CartPE scraping pipeline"""

import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.cartpe import (
    TokenScraper,
    CategoryScraper,
    ProductScraper,
    TokenExpiredException,
)
from services.database_service import (
    StoreService,
    CategoryService,
    ProductService,
    BrandService,
)
from services.image_service import ImageService
from services.push_orchestrator import PushOrchestrator
from services.csv_service import CSVService
from services.whatsapp_service import WhatsAppService
from config.settings import settings

logger = logging.getLogger(__name__)


def extract_and_update_token(store_data: dict, scraper: TokenScraper) -> tuple:
    """Extract token for a single store and update database"""
    store_id = store_data["store_id"]
    store_name = store_data["store_name"]

    try:
        token = scraper.extract_token(store_data)

        if token:
            success = StoreService.update_store_token(store_id, token)
            return (store_id, store_name, success)
        else:
            logger.warning(f"Failed to extract token for {store_name}")
            return (store_id, store_name, False)

    except Exception as e:
        logger.error(f"Error processing store {store_name}: {e}")
        WhatsAppService.send_error_notification(
            error_message=f"Token extraction failed for {store_name}",
            stack_trace=traceback.format_exc(),
        )
        return (store_id, store_name, False)


def run_token_extraction():
    """Extract tokens for all CartPE stores without tokens"""
    logger.info("=" * 80)
    logger.info("STEP 1: Token Extraction")
    logger.info("=" * 80)

    try:
        stores = StoreService.get_all_stores(store_type="cartpe")

        if not stores:
            logger.info("No CartPE stores found. Skipping token extraction.")
            return True

        # Filter stores without tokens
        stores_without_tokens = [s for s in stores if not s.get("web_token")]

        if not stores_without_tokens:
            logger.info("All stores have tokens. Skipping token extraction.")
            return True

        logger.info(f"Extracting tokens for {len(stores_without_tokens)} stores")

        scraper = TokenScraper()
        successful, failed = 0, 0

        try:
            with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
                future_to_store = {
                    executor.submit(extract_and_update_token, store, scraper): store
                    for store in stores_without_tokens
                }

                for future in as_completed(future_to_store):
                    store_id, store_name, success = future.result()

                    if success:
                        successful += 1
                        logger.info(f"✓ Token extracted: {store_name}")
                    else:
                        failed += 1
                        logger.error(f"✗ Token failed: {store_name}")

        finally:
            scraper.close()

        logger.info(
            f"Token extraction complete: {successful} successful, {failed} failed\n"
        )
        return failed == 0

    except Exception as e:
        logger.error(f"Critical error in token extraction: {e}", exc_info=True)
        WhatsAppService.send_error_notification(
            error_message="Token extraction step failed",
            stack_trace=traceback.format_exc(),
        )
        return False


def scrape_and_save_categories(store_data: dict, scraper: CategoryScraper) -> tuple:
    """Scrape categories for a single store and save to database"""
    store_id = store_data["store_id"]
    store_name = store_data["store_name"]

    try:
        categories = scraper.extract_categories(store_data)

        if categories:
            inserted_count = CategoryService.bulk_insert_categories(categories)
            logger.info(f"Saved {inserted_count} categories for {store_name}")
            return (store_id, store_name, len(categories), True)
        else:
            logger.warning(f"No categories found for {store_name}")
            return (store_id, store_name, 0, False)

    except Exception as e:
        logger.error(f"Error processing store {store_name}: {e}")
        WhatsAppService.send_error_notification(
            error_message=f"Category scraping failed for {store_name}",
            stack_trace=traceback.format_exc(),
        )
        return (store_id, store_name, 0, False)


def run_category_scraping():
    """Scrape categories for all CartPE stores"""
    logger.info("=" * 80)
    logger.info("STEP 2: Category Scraping")
    logger.info("=" * 80)

    try:
        stores = StoreService.get_all_stores(store_type="cartpe")

        if not stores:
            logger.info("No CartPE stores found in database.")
            return False

        logger.info(f"Scraping categories for {len(stores)} stores")

        scraper = CategoryScraper()
        successful, failed, total_categories = 0, 0, 0

        try:
            with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
                future_to_store = {
                    executor.submit(scrape_and_save_categories, store, scraper): store
                    for store in stores
                }

                for future in as_completed(future_to_store):
                    store_id, store_name, category_count, success = future.result()

                    if success:
                        successful += 1
                        total_categories += category_count
                        logger.info(
                            f"✓ Categories scraped: {store_name} ({category_count})"
                        )
                    else:
                        failed += 1
                        logger.error(f"✗ Categories failed: {store_name}")

        finally:
            scraper.close()

        logger.info(
            f"Category scraping complete: {total_categories} categories from {successful} stores\n"
        )
        return failed == 0

    except Exception as e:
        logger.error(f"Critical error in category scraping: {e}", exc_info=True)
        WhatsAppService.send_error_notification(
            error_message="Category scraping step failed",
            stack_trace=traceback.format_exc(),
        )
        return False


def scrape_store_products(store_data: dict) -> tuple:
    """Scrape all products for a single store with parallel category processing"""
    store_id = store_data["store_id"]
    store_name = store_data["store_name"]

    known_brands = BrandService.get_all_brands()
    total_products = 0
    failed_categories = 0

    try:
        categories = CategoryService.get_categories_by_store(store_id)

        if not categories:
            logger.warning(f"No categories found for {store_name}")
            return (store_id, store_name, 0, False)

        logger.info(
            f"Processing {len(categories)} categories for {store_name} (parallel)"
        )

        def scrape_category(category):
            """Scrape a single category"""
            scraper = ProductScraper(
                known_brands=known_brands, product_service=ProductService
            )
            try:
                ProductService.mark_category_products_inactive(
                    store_id, category["category_id"]
                )
                products = scraper.extract_products(store_data, category)

                if products:
                    ProductService.bulk_upsert_products(products)
                    logger.info(
                        f"Saved {len(products)} products for {category['category_name']}"
                    )
                    return (category["category_name"], len(products), None)
                return (category["category_name"], 0, None)

            except TokenExpiredException as e:
                return (category["category_name"], 0, e)
            except Exception as e:
                logger.error(
                    f"Error processing category {category['category_name']}: {e}"
                )
                return (category["category_name"], 0, None)
            finally:
                scraper.close()

        # Process categories in parallel (max 5 concurrent)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(scrape_category, cat): cat for cat in categories}

            for future in as_completed(futures):
                cat_name, count, error = future.result()

                if error and isinstance(error, TokenExpiredException):
                    # Token expired - refresh and retry remaining
                    logger.warning(f"Token expired for {store_name}. Re-fetching...")

                    token_scraper = TokenScraper()
                    new_token = token_scraper.extract_token(store_data)
                    token_scraper.close()

                    if new_token:
                        StoreService.update_store_token(store_id, new_token)
                        store_data["web_token"] = new_token
                        # Retry this category
                        cat = futures[future]
                        retry_name, retry_count, _ = scrape_category(cat)
                        total_products += retry_count
                    else:
                        logger.error(f"Failed to refresh token for {store_name}")
                        failed_categories += 1
                else:
                    total_products += count

        return (store_id, store_name, total_products, failed_categories == 0)

    except Exception as e:
        logger.error(f"Error processing store {store_name}: {e}")
        WhatsAppService.send_error_notification(
            error_message=f"Product scraping failed for {store_name}",
            stack_trace=traceback.format_exc(),
        )
        return (store_id, store_name, 0, False)


def run_product_scraping():
    """Scrape products for all CartPE stores"""
    logger.info("=" * 80)
    logger.info("STEP 3: Product Scraping")
    logger.info("=" * 80)

    try:
        stores = StoreService.get_all_stores(store_type="cartpe")

        if not stores:
            logger.info("No CartPE stores found in database.")
            return False

        logger.info(f"Scraping products for {len(stores)} stores")

        successful, failed, total_products = 0, 0, 0

        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            future_to_store = {
                executor.submit(scrape_store_products, store): store for store in stores
            }

            for future in as_completed(future_to_store):
                store_id, store_name, product_count, success = future.result()

                if success:
                    successful += 1
                    total_products += product_count
                    logger.info(f"✓ Products scraped: {store_name} ({product_count})")
                else:
                    failed += 1
                    logger.error(f"✗ Products failed: {store_name}")

        logger.info(
            f"Product scraping complete: {total_products} products from {successful} stores\n"
        )
        return failed == 0

    except Exception as e:
        logger.error(f"Critical error in product scraping: {e}", exc_info=True)
        WhatsAppService.send_error_notification(
            error_message="Product scraping step failed",
            stack_trace=traceback.format_exc(),
        )
        return False


def run_pipeline():
    """Main orchestrator for CartPE scraping pipeline"""
    logger.info("*" * 80)
    logger.info("CARTPE PRODUCT SCRAPER - PIPELINE")
    logger.info("*" * 80)

    try:
        if not run_token_extraction():
            logger.warning("Token extraction had failures, but continuing...")

        if not run_category_scraping():
            logger.warning("Category scraping had failures, but continuing...")

        if not run_product_scraping():
            logger.warning("Product scraping had failures")

        logger.info("=" * 80)
        logger.info("STEP 4: Image Processing")
        logger.info("=" * 80)

        try:
            ImageService.process_images(store_type="cartpe")
            ImageService.process_videos(store_type="cartpe")
        except Exception as e:
            logger.error(f"Image processing failed: {e}", exc_info=True)
            WhatsAppService.send_error_notification(
                error_message="Image processing step failed",
                stack_trace=traceback.format_exc(),
            )

        logger.info("=" * 80)
        logger.info("STEP 5: Pushing Data to Subscriptions")
        logger.info("=" * 80)

        try:
            push_results = PushOrchestrator.push_to_all_subscriptions()
            logger.info(
                f"Push complete: {push_results['success']} success, "
                f"{push_results['failed']} failed, {push_results['no_data']} no data"
            )
        except Exception as e:
            logger.error(f"Data push failed: {e}", exc_info=True)
            WhatsAppService.send_error_notification(
                error_message="Data push step failed",
                stack_trace=traceback.format_exc(),
            )

        logger.info("*" * 80)
        logger.info("CARTPE PIPELINE COMPLETE")
        logger.info("*" * 80)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in pipeline: {e}", exc_info=True)
        WhatsAppService.send_error_notification(
            error_message="CartPE pipeline crashed with fatal error",
            stack_trace=traceback.format_exc(),
        )
