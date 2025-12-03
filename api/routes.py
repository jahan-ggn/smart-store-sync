"""API routes for stores and products"""

from fastapi import APIRouter, HTTPException
from typing import List, Dict
import logging
from services.database_service import StoreService, CategoryService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["stores"])


@router.get("/stores", response_model=List[Dict])
def get_stores_with_categories():
    """
    Get all stores with their categories

    Returns:
        List of stores with nested categories
    """
    try:
        # Get all stores
        stores = StoreService.get_all_stores()

        if not stores:
            return []

        # Build response with nested categories
        result = []
        for store in stores:
            # Get categories for this store
            categories = CategoryService.get_categories_by_store(store["store_id"])

            # Format categories
            formatted_categories = [
                {
                    "category_id": cat["category_id"],
                    "category_name": cat["category_name"],
                    "category_slug": cat["category_slug"],
                    "category_url": cat["category_url"],
                }
                for cat in categories
            ]

            # Build store object
            store_data = {
                "store_id": store["store_id"],
                "store_name": store["store_name"],
                "store_slug": store["store_slug"],
                "base_url": store["base_url"],
                "categories": formatted_categories,
            }

            result.append(store_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching stores with categories: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
