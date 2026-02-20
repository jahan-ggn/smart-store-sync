"""Orchestrator for pushing data to all active subscriptions"""

import logging
from datetime import datetime
from typing import Dict
from config.database import DatabaseManager
from services.csv_service import CSVService

logger = logging.getLogger(__name__)


class PushOrchestrator:
    """Orchestrates data push to all active subscriptions"""

    @staticmethod
    def push_to_all_subscriptions(store_metrics: Dict = None) -> Dict:
        """Push data to all active subscriptions"""
        if store_metrics is None:
            store_metrics = {}

        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get all active subscriptions with whatsapp number
                cursor.execute(
                    """SELECT id, buyer_domain, whatsapp_number 
                       FROM api_subscriptions 
                       WHERE expires_at > NOW()"""
                )
                subscriptions = cursor.fetchall()

                logger.info(f"Found {len(subscriptions)} active subscriptions")

                results = {
                    "total": len(subscriptions),
                    "success": 0,
                    "failed": 0,
                    "no_data": 0,
                    "details": [],
                }

                for sub in subscriptions:
                    subscription_id = sub["id"]
                    buyer_domain = sub["buyer_domain"]
                    whatsapp_number = sub.get("whatsapp_number")

                    try:
                        # Generate CSV
                        csv_path = CSVService.generate_csv_for_subscription(
                            subscription_id
                        )

                        if not csv_path:
                            results["no_data"] += 1
                            results["details"].append(
                                {
                                    "subscription_id": subscription_id,
                                    "domain": buyer_domain,
                                    "status": "no_data",
                                }
                            )
                            continue

                        logger.info(f"CSV generated for {buyer_domain}: {csv_path}")

                        # Push to WordPress
                        push_result = CSVService.push_csv_in_chunks(
                            csv_path, subscription_id, chunk_size=300
                        )

                        # Update last_push_at only if all chunks succeeded
                        if push_result["failed"] == 0:
                            cursor.execute(
                                "UPDATE subscription_permissions SET last_push_at = %s WHERE subscription_id = %s",
                                (datetime.now(), subscription_id),
                            )
                            conn.commit()

                            # Send analytics WhatsApp to subscriber
                            if store_metrics and whatsapp_number:
                                # Get store_ids this subscription has access to
                                cursor.execute(
                                    "SELECT store_id FROM subscription_permissions WHERE subscription_id = %s",
                                    (subscription_id,),
                                )
                                sub_store_ids = {
                                    row["store_id"] for row in cursor.fetchall()
                                }

                                # Filter metrics to only relevant stores
                                relevant_metrics = {
                                    sid: data
                                    for sid, data in store_metrics.items()
                                    if sid in sub_store_ids
                                    and data["metrics"]["total"] > 0
                                }

                                if relevant_metrics:
                                    WhatsAppService.send_sync_analytics(
                                        to_number=whatsapp_number,
                                        buyer_domain=buyer_domain,
                                        store_metrics=relevant_metrics,
                                    )
                        else:
                            logger.warning(
                                f"Skipping last_push_at update for {buyer_domain}: "
                                f"{push_result['failed']} chunks failed"
                            )

                        results["success"] += 1
                        results["details"].append(
                            {
                                "subscription_id": subscription_id,
                                "domain": buyer_domain,
                                "status": "success",
                                "push_result": push_result,
                            }
                        )

                    except Exception as e:
                        results["failed"] += 1
                        results["details"].append(
                            {
                                "subscription_id": subscription_id,
                                "domain": buyer_domain,
                                "status": "failed",
                                "error": str(e),
                            }
                        )
                        logger.error(f"Failed to push to {buyer_domain}: {str(e)}")

                logger.info(
                    f"Push completed: {results['success']} success, "
                    f"{results['failed']} failed, {results['no_data']} no data"
                )

                return results

        except Exception as e:
            logger.error(f"Error in push orchestrator: {str(e)}")
            raise
