"""One-time script to load brands from brands.txt into database"""

import logging
from utils.logger import setup_logger
from config.database import DatabaseManager

setup_logger("brands")
logger = logging.getLogger(__name__)


def load_brands_from_file(file_path: str = "brands.txt"):
    """
    Load brands from text file and insert into database

    Args:
        file_path: Path to brands.txt file
    """
    logger.info("=" * 80)
    logger.info("Loading Brands into Database")
    logger.info("=" * 80)

    try:
        # Read brands from file
        with open(file_path, "r", encoding="utf-8") as f:
            brands = [line.strip().strip('"') for line in f if line.strip()]

        logger.info(f"Found {len(brands)} brands in file")

        # Truncate table first
        with DatabaseManager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM brands")
            conn.commit()

        # Prepare insert query
        query = "INSERT IGNORE INTO brands (brand_name) VALUES (%s)"
        data = [(brand,) for brand in brands if brand]

        # Bulk insert
        rows_affected = DatabaseManager.execute_many(query, data)

        logger.info(f"Successfully inserted {rows_affected} brands into database")
        logger.info("=" * 80)

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error loading brands: {e}", exc_info=True)


if __name__ == "__main__":
    load_brands_from_file()
