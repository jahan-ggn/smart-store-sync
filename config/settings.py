"""Application configuration settings"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Centralized configuration for the application"""

    # Database Configuration
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "smart_store_sync")
    ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

    # Connection Pool Settings
    POOL_NAME = "smart_store_sync_pool"
    POOL_SIZE = int(os.getenv("POOL_SIZE", 10))

    # Scraping Configuration
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", 0.5))
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR = "logs"
    LOG_FILE = "scraper.log"

    # R2 Configuration
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
    R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL", "")
    R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "")
    R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

    # DEZGO (background removal)
    DEZGO_API_KEY = os.getenv("DEZGO_API_KEY", "")

    # Twilio Configuration
    TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
    TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
    TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")
    ADMIN_WHATSAPP_NUMBER = os.getenv("ADMIN_WHATSAPP_NUMBER", "")


settings = Settings()
