"""Main entry point for the complete scraping pipeline"""

import logging
from utils.logger import setup_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.token_scraper import TokenScraper
from scrapers.category_scraper import CategoryScraper
from scrapers.product_scraper import ProductScraper, TokenExpiredException
from services.database_service import StoreService, CategoryService, ProductService
from config.settings import settings
from services.push_orchestrator import PushOrchestrator
from services.csv_service import CSVService
from services.image_service import ImageService


setup_logger()
logger = logging.getLogger(__name__)


# ============================================================================
# STEP 1: TOKEN EXTRACTION
# ============================================================================


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
        return (store_id, store_name, False)


def run_token_extraction():
    """Extract tokens for all stores without tokens"""
    logger.info("=" * 80)
    logger.info("STEP 1: Token Extraction")
    logger.info("=" * 80)

    stores = StoreService.get_all_stores()

    if not stores:
        logger.info("All stores have tokens. Skipping token extraction.")
        return True

    logger.info(f"Extracting tokens for {len(stores)} stores")

    scraper = TokenScraper()
    successful = 0
    failed = 0

    try:
        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            future_to_store = {
                executor.submit(extract_and_update_token, store, scraper): store
                for store in stores
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


# ============================================================================
# STEP 2: CATEGORY SCRAPING
# ============================================================================


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
        return (store_id, store_name, 0, False)


def run_category_scraping():
    """Scrape categories for all stores"""
    logger.info("=" * 80)
    logger.info("STEP 2: Category Scraping")
    logger.info("=" * 80)

    stores = StoreService.get_all_stores()

    if not stores:
        logger.info("No stores found in database.")
        return False

    logger.info(f"Scraping categories for {len(stores)} stores")

    scraper = CategoryScraper()
    successful = 0
    failed = 0
    total_categories = 0

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


# ============================================================================
# STEP 3: PRODUCT SCRAPING
# ============================================================================


def scrape_store_products(store_data: dict) -> tuple:
    """Scrape all products for a single store"""
    store_id = store_data["store_id"]
    store_name = store_data["store_name"]

    scraper = ProductScraper()
    total_products = 0

    try:
        categories = CategoryService.get_categories_by_store(store_id)

        if not categories:
            logger.warning(f"No categories found for {store_name}")
            return (store_id, store_name, 0, False)

        logger.info(f"Processing {len(categories)} categories for {store_name}")

        for category in categories:
            try:
                # Mark all products in this category as inactive before scraping
                ProductService.mark_category_products_inactive(
                    store_id, category["category_id"]
                )

                # Extract products
                products = scraper.extract_products(store_data, category)

                if products:
                    ProductService.bulk_upsert_products(products)
                    total_products += len(products)
                    logger.info(
                        f"Saved {len(products)} products for {category['category_name']}"
                    )

            except TokenExpiredException:
                logger.warning(f"Token expired for {store_name}. Re-fetching token...")

                token_scraper = TokenScraper()
                new_token = token_scraper.extract_token(store_data)
                token_scraper.close()

                if new_token:
                    StoreService.update_store_token(store_id, new_token)
                    store_data["web_token"] = new_token
                    logger.info(f"Token refreshed for {store_name}. Retrying...")

                    products = scraper.extract_products(store_data, category)
                    if products:
                        ProductService.bulk_upsert_products(products)
                        total_products += len(products)
                else:
                    logger.error(f"Failed to refresh token for {store_name}")
                    break

            except Exception as e:
                logger.error(
                    f"Error processing category {category['category_name']}: {e}"
                )
                continue

        scraper.close()
        return (store_id, store_name, total_products, True)

    except Exception as e:
        logger.error(f"Error processing store {store_name}: {e}")
        scraper.close()
        return (store_id, store_name, 0, False)


def run_product_scraping():
    """Scrape products for all stores"""
    logger.info("=" * 80)
    logger.info("STEP 3: Product Scraping")
    logger.info("=" * 80)

    stores = StoreService.get_all_stores()

    if not stores:
        logger.info("No stores found in database.")
        return False

    logger.info(f"Scraping products for {len(stores)} stores")

    successful = 0
    failed = 0
    total_products = 0

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


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================


def main():
    """Main orchestrator for the complete scraping pipeline"""
    logger.info("*" * 80)
    logger.info("CARTPE PRODUCT SCRAPER - COMPLETE PIPELINE")
    logger.info("*" * 80)
    logger.info("")

    try:
        CSVService.cleanup_old_csvs()

        # Step 1: Extract tokens
        if not run_token_extraction():
            logger.warning("Token extraction had failures, but continuing...")

        # Step 2: Scrape categories
        if not run_category_scraping():
            logger.warning("Category scraping had failures, but continuing...")

        # Step 3: Scrape products
        if not run_product_scraping():
            logger.warning("Product scraping had failures")

        # Step 4: Process images and upload to R2
        logger.info("=" * 80)
        logger.info("STEP 4: Image Processing")
        logger.info("=" * 80)
        ImageService.process_all_products()

        # Step 4: Push data to subscriptions
        logger.info("=" * 80)
        logger.info("STEP 5: Pushing Data to Subscriptions")
        logger.info("=" * 80)

        push_results = PushOrchestrator.push_to_all_subscriptions()

        logger.info(
            f"Push complete: {push_results['success']} success, "
            f"{push_results['failed']} failed, {push_results['no_data']} no data"
        )

        logger.info("*" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("*" * 80)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in pipeline: {e}", exc_info=True)


if __name__ == "__main__":
    main()
