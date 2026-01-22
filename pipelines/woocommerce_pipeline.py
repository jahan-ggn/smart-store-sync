"""WooCommerce scraping pipeline"""

import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.woocommerce import CategoryScraper, ProductScraper
from services.database_service import StoreService, CategoryService, ProductService
from services.image_service import ImageService
from services.push_orchestrator import PushOrchestrator
from services.csv_service import CSVService
from services.whatsapp_service import WhatsAppService
from config.settings import settings

logger = logging.getLogger(__name__)


def scrape_store_categories(store_data: dict, scraper: CategoryScraper) -> tuple:
    """Scrape categories for a single store"""
    store_id = store_data["store_id"]
    store_name = store_data["store_name"]

    try:
        categories = scraper.extract_categories(store_data)
        if categories:
            CategoryService.bulk_insert_categories(categories)
            return (store_id, store_name, len(categories), True)
        return (store_id, store_name, 0, False)
    except Exception as e:
        logger.error(f"Error scraping categories for {store_name}: {e}")
        WhatsAppService.send_error_notification(
            error_message=f"Category scraping failed for {store_name}",
            stack_trace=traceback.format_exc(),
        )
        return (store_id, store_name, 0, False)


def scrape_store_products(store_data: dict) -> tuple:
    """Scrape products for a single store"""
    store_id = store_data["store_id"]
    store_name = store_data["store_name"]

    scraper = ProductScraper()
    try:
        ProductService.mark_store_products_inactive(store_id)
        products = scraper.extract_products(store_data)

        if not products:
            return (store_id, store_name, 0, False)

        ProductService.bulk_upsert_products(products)
        return (store_id, store_name, len(products), True)

    except Exception as e:
        logger.error(f"Error scraping products for {store_name}: {e}")
        WhatsAppService.send_error_notification(
            error_message=f"Product scraping failed for {store_name}",
            stack_trace=traceback.format_exc(),
        )
        return (store_id, store_name, 0, False)
    finally:
        scraper.close()


def run_category_scraping():
    """Scrape categories for all WooCommerce stores"""
    logger.info("=" * 80)
    logger.info("STEP 1: Category Scraping")
    logger.info("=" * 80)

    stores = StoreService.get_all_stores(store_type="woocommerce")
    if not stores:
        logger.info("No WooCommerce stores found in database.")
        return False

    scraper = CategoryScraper()
    successful, failed = 0, 0

    try:
        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            futures = {
                executor.submit(scrape_store_categories, store, scraper): store
                for store in stores
            }
            for future in as_completed(futures):
                store_id, store_name, count, ok = future.result()
                if ok:
                    successful += 1
                    logger.info(f"✓ {store_name}: {count} categories")
                else:
                    failed += 1
                    logger.error(f"✗ {store_name}: failed")
    finally:
        scraper.close()

    logger.info(f"Category scraping complete: {successful} success, {failed} failed\n")
    return failed == 0


def run_product_scraping():
    """Scrape products for all WooCommerce stores"""
    logger.info("=" * 80)
    logger.info("STEP 2: Product Scraping")
    logger.info("=" * 80)

    stores = StoreService.get_all_stores(store_type="woocommerce")
    if not stores:
        logger.info("No WooCommerce stores found in database.")
        return False

    successful, failed, total_products = 0, 0, 0

    with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_store_products, store): store for store in stores
        }
        for future in as_completed(futures):
            store_id, store_name, count, ok = future.result()
            if ok:
                successful += 1
                total_products += count
                logger.info(f"✓ {store_name}: {count} products")
            else:
                failed += 1
                logger.error(f"✗ {store_name}: failed")

    logger.info(
        f"Product scraping complete: {total_products} products from {successful} stores\n"
    )
    return failed == 0


def run_pipeline():
    """Main orchestrator for WooCommerce scraping pipeline"""
    logger.info("*" * 80)
    logger.info("WOOCOMMERCE PRODUCT SCRAPER - PIPELINE")
    logger.info("*" * 80)

    try:
        if not run_category_scraping():
            logger.warning("Category scraping had failures, but continuing...")

        if not run_product_scraping():
            logger.warning("Product scraping had failures")

        logger.info("=" * 80)
        logger.info("STEP 3: Image Processing")
        logger.info("=" * 80)

        try:
            ImageService.process_images(store_type="woocommerce")
            ImageService.process_videos(store_type="woocommerce")
        except Exception as e:
            logger.error(f"Image processing failed: {e}", exc_info=True)
            WhatsAppService.send_error_notification(
                error_message="Image processing step failed",
                stack_trace=traceback.format_exc(),
            )

        logger.info("=" * 80)
        logger.info("STEP 4: Pushing Data to Subscriptions")
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
        logger.info("WOOCOMMERCE PIPELINE COMPLETE")
        logger.info("*" * 80)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in pipeline: {e}", exc_info=True)
        WhatsAppService.send_error_notification(
            error_message="WooCommerce pipeline crashed with fatal error",
            stack_trace=traceback.format_exc(),
        )
