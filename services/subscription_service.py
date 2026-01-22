"""Subscription service for managing API subscriptions"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict
from config.database import DatabaseManager

logger = logging.getLogger(__name__)


class SubscriptionService:
    """Service for managing subscriptions"""

    @staticmethod
    def create_subscription(
        buyer_email: str,
        buyer_domain: str,
        plan_name: str,
        plan_duration: str,
        whatsapp_number: str,
    ) -> Dict:
        """Create a new subscription and generate token"""
        try:
            if plan_name not in ["starter", "pro"]:
                raise ValueError(f"Invalid plan_name: {plan_name}")

            if plan_duration not in ["monthly", "yearly"]:
                raise ValueError(f"Invalid plan_duration: {plan_duration}")

            token = str(uuid.uuid4())
            created_at = datetime.now()

            if plan_duration == "monthly":
                expires_at = created_at + timedelta(days=30)
            else:
                expires_at = created_at + timedelta(days=365)

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                query = """
                    INSERT INTO api_subscriptions 
                    (token, buyer_email, whatsapp_number, buyer_domain, plan_name, plan_duration, created_at, expires_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                cursor.execute(
                    query,
                    (
                        token,
                        buyer_email,
                        whatsapp_number,
                        buyer_domain,
                        plan_name,
                        plan_duration,
                        created_at,
                        expires_at,
                    ),
                )

                subscription_id = cursor.lastrowid
                conn.commit()

                logger.info(f"Created subscription {subscription_id} for {buyer_email}")

                return {
                    "subscription_id": subscription_id,
                    "token": token,
                    "buyer_email": buyer_email,
                    "whatsapp_number": whatsapp_number,
                    "buyer_domain": buyer_domain,
                    "plan_name": plan_name,
                    "plan_duration": plan_duration,
                    "created_at": created_at.isoformat(),
                    "expires_at": expires_at.isoformat(),
                }

        except Exception as e:
            logger.error(f"Error creating subscription: {str(e)}")
            raise

    @staticmethod
    def add_subscription_permissions(
        token: str, buyer_domain: str, store_ids: List[int]
    ) -> Dict:
        """Set store permissions - validates token matches domain"""
        try:
            if not store_ids:
                raise ValueError("store_ids cannot be empty")

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get subscription by domain and verify token
                cursor.execute(
                    """SELECT id, token FROM api_subscriptions 
                    WHERE buyer_domain = %s AND expires_at > NOW()
                    ORDER BY created_at DESC LIMIT 1""",
                    (buyer_domain,),
                )
                subscription = cursor.fetchone()

                if not subscription:
                    raise ValueError(
                        f"No active subscription found for domain: {buyer_domain}"
                    )

                if subscription["token"] != token:
                    raise ValueError("Invalid token for this domain")

                subscription_id = subscription["id"]

                # Validate that all store_ids exist
                placeholders = ",".join(["%s"] * len(store_ids))
                cursor.execute(
                    f"SELECT store_id FROM stores WHERE store_id IN ({placeholders})",
                    store_ids,
                )
                valid_stores = {row["store_id"] for row in cursor.fetchall()}

                invalid_stores = set(store_ids) - valid_stores
                if invalid_stores:
                    raise ValueError(f"Invalid store IDs: {invalid_stores}")

                # Delete existing permissions
                cursor.execute(
                    "DELETE FROM subscription_permissions WHERE subscription_id = %s",
                    (subscription_id,),
                )

                # Insert new permissions
                for store_id in store_ids:
                    cursor.execute(
                        "INSERT INTO subscription_permissions (subscription_id, store_id) VALUES (%s, %s)",
                        (subscription_id, store_id),
                    )

                conn.commit()

                logger.info(
                    f"Set {len(store_ids)} store permissions for subscription {subscription_id}"
                )

                return {
                    "subscription_id": subscription_id,
                    "buyer_domain": buyer_domain,
                    "total_stores": len(store_ids),
                }

        except Exception as e:
            logger.error(f"Error setting permissions: {str(e)}")
            raise

    @staticmethod
    def store_woocommerce_credentials(
        token: str, buyer_domain: str, consumer_key: str, consumer_secret: str
    ) -> Dict:
        """Store WooCommerce API credentials for a subscription"""
        try:
            if not consumer_key or not consumer_key.startswith("ck_"):
                raise ValueError("Invalid consumer_key format. Must start with 'ck_'")

            if not consumer_secret or not consumer_secret.startswith("cs_"):
                raise ValueError(
                    "Invalid consumer_secret format. Must start with 'cs_'"
                )

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                cursor.execute(
                    """SELECT id, token FROM api_subscriptions 
                    WHERE buyer_domain = %s AND expires_at > NOW()
                    ORDER BY created_at DESC LIMIT 1""",
                    (buyer_domain,),
                )
                subscription = cursor.fetchone()

                if not subscription:
                    raise ValueError(
                        f"No active subscription found for domain: {buyer_domain}"
                    )

                if subscription["token"] != token:
                    raise ValueError("Invalid token for this domain")

                subscription_id = subscription["id"]

                query = """
                    INSERT INTO woocommerce_credentials 
                    (subscription_id, consumer_key, consumer_secret)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    consumer_key = VALUES(consumer_key),
                    consumer_secret = VALUES(consumer_secret)
                """

                cursor.execute(query, (subscription_id, consumer_key, consumer_secret))
                conn.commit()

                logger.info(
                    f"Stored WooCommerce credentials for subscription {subscription_id}"
                )

                return {
                    "subscription_id": subscription_id,
                    "buyer_domain": buyer_domain,
                    "message": "WooCommerce credentials stored successfully",
                }

        except Exception as e:
            logger.error(f"Error storing WooCommerce credentials: {str(e)}")
            raise

    @staticmethod
    def get_subscription_status(token: str, buyer_domain: str) -> Dict:
        """Get subscription status and details"""
        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                cursor.execute(
                    """SELECT id, token, plan_name, plan_duration, expires_at 
                    FROM api_subscriptions 
                    WHERE buyer_domain = %s AND expires_at > NOW()
                    ORDER BY created_at DESC LIMIT 1""",
                    (buyer_domain,),
                )
                subscription = cursor.fetchone()

                if not subscription:
                    raise ValueError(
                        f"No active subscription found for domain: {buyer_domain}"
                    )

                if subscription["token"] != token:
                    raise ValueError("Invalid token for this domain")

                subscription_id = subscription["id"]

                # Get selected stores
                cursor.execute(
                    """SELECT sp.store_id, s.store_name, s.store_type
                    FROM subscription_permissions sp
                    JOIN stores s ON sp.store_id = s.store_id
                    WHERE sp.subscription_id = %s""",
                    (subscription_id,),
                )
                stores = cursor.fetchall()

                return {
                    "plan_name": subscription["plan_name"],
                    "plan_duration": subscription["plan_duration"],
                    "expires_at": subscription["expires_at"].isoformat(),
                    "selected_stores": [
                        {
                            "store_id": s["store_id"],
                            "store_name": s["store_name"],
                            "store_type": s["store_type"],
                        }
                        for s in stores
                    ],
                }

        except Exception as e:
            logger.error(f"Error getting subscription status: {str(e)}")
            raise
