"""API routes for stores and subscriptions"""

import logging
from fastapi import APIRouter, HTTPException, Header, BackgroundTasks
from typing import List, Dict
from pydantic import BaseModel, EmailStr, field_validator
from config.settings import settings
from services.database_service import StoreService, CategoryService
from scrapers.cartpe import CategoryScraper as CartPeCategoryScraper
from scrapers.woocommerce import CategoryScraper as WooCategoryScraper

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["stores"])


# Pydantic models
class StoreCreateRequest(BaseModel):
    store_type: str  # "cartpe" or "woocommerce"
    store_name: str
    store_slug: str
    base_url: str
    api_endpoint: str = None


class SubscriptionCreateRequest(BaseModel):
    buyer_email: EmailStr
    buyer_domain: str
    plan_name: str
    plan_duration: str
    whatsapp_number: str

    @field_validator("whatsapp_number")
    @classmethod
    def validate_whatsapp(cls, v: str) -> str:
        cleaned = "".join(filter(str.isdigit, v))
        if len(cleaned) != 10:
            raise ValueError("WhatsApp number must be exactly 10 digits")
        return f"whatsapp:+91{cleaned}"


class PermissionAddRequest(BaseModel):
    token: str
    buyer_domain: str
    store_ids: List[int]


class WooCommerceCredentialsRequest(BaseModel):
    token: str
    buyer_domain: str
    consumer_key: str
    consumer_secret: str


class SubscriptionStatusRequest(BaseModel):
    token: str
    buyer_domain: str


# Background tasks
def fetch_and_store_categories_cartpe(store_data: dict):
    """Background task to fetch categories for a newly created CartPE store"""
    try:
        scraper = CartPeCategoryScraper()
        categories = scraper.extract_categories(store_data)
        if categories:
            CategoryService.bulk_insert_categories(categories)
        scraper.close()
    except Exception as e:
        logger.error(f"Error fetching CartPE categories in background: {e}")


def fetch_and_store_categories_woocommerce(store_data: dict):
    """Background task to fetch categories for a newly created WooCommerce store"""
    try:
        scraper = WooCategoryScraper()
        categories = scraper.extract_categories(store_data)
        if categories:
            CategoryService.bulk_insert_categories(categories)
        scraper.close()
    except Exception as e:
        logger.error(f"Error fetching WooCommerce categories in background: {e}")


# Routes
@router.get("/stores", response_model=List[Dict])
def get_stores_with_categories(store_type: str = None):
    """Get all stores with their categories, optionally filtered by store_type"""
    try:
        stores = StoreService.get_all_stores(store_type=store_type)

        if not stores:
            return []

        result = []
        for store in stores:
            categories = CategoryService.get_categories_by_store(store["store_id"])

            store_data = {
                "store_id": store["store_id"],
                "store_type": store["store_type"],
                "store_name": store["store_name"],
                "store_slug": store["store_slug"],
                "base_url": store["base_url"],
                "categories": [
                    {
                        "category_id": cat["category_id"],
                        "category_name": cat["category_name"],
                        "category_slug": cat["category_slug"],
                    }
                    for cat in categories
                ],
            }
            result.append(store_data)

        return result

    except Exception as e:
        logger.error(f"Error fetching stores with categories: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/stores")
async def create_store(request: StoreCreateRequest, background_tasks: BackgroundTasks):
    """Create a new store"""
    try:
        if request.store_type not in ["cartpe", "woocommerce"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid store_type. Must be 'cartpe' or 'woocommerce'",
            )

        store_data = {
            "store_type": request.store_type,
            "store_name": request.store_name,
            "store_slug": request.store_slug,
            "base_url": request.base_url,
            "api_endpoint": request.api_endpoint,
        }

        result = StoreService.create_store(store_data)

        # Add background task based on store type
        bg_store_data = {**store_data, "store_id": result["store_id"]}

        if request.store_type == "cartpe":
            background_tasks.add_task(fetch_and_store_categories_cartpe, bg_store_data)
        else:
            background_tasks.add_task(
                fetch_and_store_categories_woocommerce, bg_store_data
            )

        return {"success": True, "data": result}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating store: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/subscriptions/register")
async def register_subscription(
    request: SubscriptionCreateRequest, api_key: str = Header(None)
):
    """Register a new subscription"""
    if not api_key or api_key != settings.ADMIN_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    try:
        from services.subscription_service import SubscriptionService

        if request.plan_name not in ["starter", "pro"]:
            raise ValueError("Invalid plan_name")
        if request.plan_duration not in ["monthly", "yearly"]:
            raise ValueError("Invalid plan_duration")

        subscription = SubscriptionService.create_subscription(
            buyer_email=request.buyer_email,
            whatsapp_number=request.whatsapp_number,
            buyer_domain=request.buyer_domain.rstrip("/"),
            plan_name=request.plan_name,
            plan_duration=request.plan_duration,
        )

        return {"success": True, "data": subscription}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error registering subscription: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/subscriptions/permissions")
async def add_permissions(request: PermissionAddRequest):
    """Add store permissions for subscription"""
    try:
        from services.subscription_service import SubscriptionService

        result = SubscriptionService.add_subscription_permissions(
            token=request.token,
            buyer_domain=request.buyer_domain,
            store_ids=request.store_ids,
        )

        return {"success": True, "data": result}

    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding permissions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/subscriptions/woocommerce")
async def store_woocommerce_credentials(request: WooCommerceCredentialsRequest):
    """Store WooCommerce credentials for subscription"""
    try:
        from services.subscription_service import SubscriptionService

        result = SubscriptionService.store_woocommerce_credentials(
            token=request.token,
            buyer_domain=request.buyer_domain,
            consumer_key=request.consumer_key,
            consumer_secret=request.consumer_secret,
        )

        return {"success": True, "data": result}

    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error storing credentials: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/subscriptions/status")
async def get_subscription_status(request: SubscriptionStatusRequest):
    """Get subscription status"""
    try:
        from services.subscription_service import SubscriptionService

        result = SubscriptionService.get_subscription_status(
            token=request.token, buyer_domain=request.buyer_domain
        )

        return {"success": True, "data": result}

    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
