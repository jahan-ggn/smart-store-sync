"""FastAPI application for CartPE scraper"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import router

app = FastAPI(
    title="CartPE Product Scraper API",
    description="API for accessing scraped product data",
    version="1.0.0",
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router)


@app.get("/")
def read_root():
    return {
        "message": "CartPE Product Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "stores": "/api/stores",
            "register_subscription": "/api/subscriptions/register",
            "add_permissions": "/api/subscriptions/permissions",
            "store_woocommerce": "/api/subscriptions/woocommerce",
            "subscription_status": "/api/subscriptions/status",
        },
    }
