"""WhatsApp notification service using Twilio"""

import logging
from twilio.rest import Client
from config.settings import settings

logger = logging.getLogger(__name__)


class WhatsAppService:
    """Service for sending WhatsApp notifications via Twilio"""

    def __init__(self):
        """Initialize Twilio client"""
        self.client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
        self.from_number = settings.TWILIO_WHATSAPP_FROM

    def send_message(self, to_number: str, message: str) -> bool:
        """Send WhatsApp message"""
        try:
            message = self.client.messages.create(
                from_=self.from_number, body=message, to=to_number
            )
            logger.info(f"WhatsApp message sent to {to_number}: {message.sid}")
            return True
        except Exception as e:
            logger.error(f"Error sending WhatsApp to {to_number}: {str(e)}")
            return False

    @staticmethod
    def send_error_notification(error_message: str, stack_trace: str = None):
        """Send error notification to admin"""
        service = WhatsAppService()

        message = f"🚨 *Smart Store Sync Error Alert*\n\n"
        message += f"*Error:* {error_message}\n\n"

        if stack_trace:
            trace_preview = stack_trace[:1000]
            message += f"*Stack Trace:*\n```{trace_preview}```"

        service.send_message(settings.ADMIN_WHATSAPP_NUMBER, message)

    @staticmethod
    def send_payment_reminder(
        buyer_name: str, expiry_date: str, days_left: int, to_number: str
    ):
        """Send payment reminder to subscriber"""
        service = WhatsAppService()

        message = f"Hi {buyer_name},\n\n"
        message += f"Your Smart Store Sync subscription expires in *{days_left} days* ({expiry_date}).\n\n"
        message += "Renew now to continue receiving product updates.\n\n"
        message += "Reply RENEW for payment link."

        service.send_message(to_number, message)
