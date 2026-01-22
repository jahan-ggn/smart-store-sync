"""Scheduler for payment reminders"""

import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from config.database import DatabaseManager
from services.whatsapp_service import WhatsAppService

logger = logging.getLogger(__name__)


class ReminderScheduler:
    """Scheduler for sending payment reminders"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def check_and_send_reminders(self):
        """Check for expiring subscriptions and send reminders"""
        try:
            logger.info("Checking for expiring subscriptions...")

            with DatabaseManager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                today = datetime.now()

                # Check for subscriptions expiring in 7, 3, and 1 days
                for days in [7, 3, 1]:
                    target_date = today + timedelta(days=days)

                    query = """
                        SELECT buyer_email, expires_at, whatsapp_number
                        FROM api_subscriptions
                        WHERE expires_at > NOW()
                        AND DATE(expires_at) = DATE(%s)
                        AND whatsapp_number IS NOT NULL
                    """

                    cursor.execute(query, (target_date,))
                    subscriptions = cursor.fetchall()

                    for sub in subscriptions:
                        buyer_name = sub["buyer_email"].split("@")[0]
                        expiry_date = sub["expires_at"].strftime("%d %B %Y")

                        WhatsAppService.send_payment_reminder(
                            buyer_name=buyer_name,
                            expiry_date=expiry_date,
                            days_left=days,
                            to_number=sub["whatsapp_number"],
                        )

                        logger.info(f"Sent {days}-day reminder to {sub['buyer_email']}")

            logger.info("Reminder check complete")

        except Exception as e:
            logger.error(f"Error in reminder scheduler: {str(e)}")
            WhatsAppService.send_error_notification(
                error_message=f"Reminder scheduler failed: {str(e)}",
                stack_trace=str(e),
            )

    def start(self):
        """Start the scheduler - runs daily at 11 AM"""
        logger.info("Starting reminder scheduler...")
        self.scheduler.add_job(self.check_and_send_reminders, "cron", hour=11, minute=0)
        self.scheduler.start()
        logger.info("Reminder scheduler started - runs daily at 11 AM")

    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Reminder scheduler stopped")
