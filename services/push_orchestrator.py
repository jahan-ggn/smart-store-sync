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
    def push_to_all_subscriptions() -> Dict:
        """Push data to all active subscriptions"""
        try:
            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get all active subscriptions
                cursor.execute("""SELECT id, buyer_domain 
                       FROM api_subscriptions 
                       WHERE expires_at > NOW()""")
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

                        # Push to WordPress
                        push_result = CSVService.push_csv_in_chunks(
                            csv_path, subscription_id, chunk_size=300
                        )

                        # Update last_push_at
                        cursor.execute(
                            "UPDATE api_subscriptions SET last_push_at = %s WHERE id = %s",
                            (datetime.now(), subscription_id),
                        )
                        conn.commit()

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
