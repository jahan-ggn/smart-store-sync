"""FastAPI application for Smart Store Sync"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api.routes import router
from services.reminder_scheduler import ReminderScheduler
from utils.logger import setup_logger

setup_logger("api")
logger = logging.getLogger(__name__)

reminder_scheduler = ReminderScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    reminder_scheduler.start()
    yield
    reminder_scheduler.stop()


app = FastAPI(
    title="Smart Store Sync API",
    description="API for managing stores, subscriptions, and product data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
def read_root():
    return {
        "message": "Smart Store Sync API",
        "version": "1.0.0",
        "endpoints": {
            "stores": "/api/stores",
            "register_subscription": "/api/subscriptions/register",
            "add_permissions": "/api/subscriptions/permissions",
            "store_woocommerce": "/api/subscriptions/woocommerce",
            "subscription_status": "/api/subscriptions/status",
        },
    }
