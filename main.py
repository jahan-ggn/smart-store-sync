"""Main entry point for the Smart Store Sync scraping pipeline"""

import argparse
from services.csv_service import CSVService
from utils.logger import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description="Smart Store Sync - Product Scraping Pipeline"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["cartpe", "woocommerce", "all"],
        required=True,
        help="Source to scrape: cartpe, woocommerce, or all",
    )

    args = parser.parse_args()

    # Setup logger based on source
    if args.source == "all":
        setup_logger("scraper")
    else:
        setup_logger(args.source)

    CSVService.cleanup_old_csvs()

    if args.source == "cartpe":
        from pipelines.cartpe_pipeline import run_pipeline

        run_pipeline()

    elif args.source == "woocommerce":
        from pipelines.woocommerce_pipeline import run_pipeline

        run_pipeline()

    elif args.source == "all":
        from pipelines.cartpe_pipeline import run_pipeline as run_cartpe
        from pipelines.woocommerce_pipeline import run_pipeline as run_woocommerce

        run_cartpe()
        run_woocommerce()


if __name__ == "__main__":
    main()
