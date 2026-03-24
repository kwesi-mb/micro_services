"""
Email Service - Reads from email.queue, renders templates, sends via SMTP/SendGrid.
Implements retry with exponential backoff and dead-letter queue routing.
"""

import asyncio
import json
import logging
import smtplib
import ssl
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from email.mine.multipart import MINEMultipart
from email.mine.text import MINEText
from typing import Optional

import aio_pika
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.redis_client import redis_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

# Email Sender

async def send_smtp_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via SMTP. Returns True on success."""
    try:
        msg = MINEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = settings.smtp_from_email
        msg["To"] = to_email
        msg.attach(MINETtext(html_body, "html"))

        context = ssl.create_default_context()
        with smtplin.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.ehlo()
            if settings.smtp_user_tls:
                server.starttls(context=context)
            if settings.smtp_username and settings.smtp_password:
                server.login(settings.smtp_username, settings.smtp_password)
            server.sendmail(settings.smtp_from_email, to_email, msg.as_string())

        logger.info(f"Email sent to {to_email}")
        return True
    except Exception as e:
        logger.error(f"SMTP error sending to {to_email}: {e}")
        return False

async def send_sendgrid_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via SendGrid API."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        try: 
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {settings.sendgrid_api_key}"},
                json={
                    "personalizations": [{"to": [{"email": to_email}]}],
                    "from": {"email": settings.smtp_from_email},
                    "subject": subject,
                    "content": [{"type": "text/html", "value": html_body}],
                },
            )
            if resp.status_code in (200, 202):
                logger.info(f"SendGrid email sent to {to_email}")
                return True
            logger.error(f"SendGrid error {resp.status_code}: {resp.text}")
            return False
        except Exception as e:
            logger.error(f"SendGrid request failed: {e}")
            return False

async def deliver_email(to_email: str, subject: str, html_body: str) -> bool:
    """Route to configured provider."""
    if settings.email_provider == "sendgrid" and settings.sendgrid_api_key:
        return await send_sendgrid_email(to_email, subject, html_body)
    return await send_smtp_email(to_email, subject, html_body)


# Template & User fetching

async def fetch_user(user_id: str) -> Optional[dict]:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(f"{settings.user_service_url}/api/v1/users/{user_id}")
            if resp.status_code == 200:
                return resp.json().get("data")
        except Exception as e:
            logger.error(f"Failed to fetch user {user_id}: {e}")
    return None

async def render_template(template_code: str, variables: dict) -> Optional[dict]:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.post(
                f"{settings.template_service_url}/api/v1/templates/{template_code}/render",
                json={"variables": variables},
            )
            if resp.status_code ==200:
                return resp.json().get("data")
        except Exception as e:
            logger.error(f"Failed to render template {template_code}: {e}")
    return None


async def update_notfication_status(notification_id: str, status: str, error: Optional[str] = None):
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(
                f"{settings.api_gateway_url}/api/v1/notifications/email/status/",
                json={
                    "notification_id": notification_id,
                    "status": status,
                    "timestamp": datetime.now(timezone.utc)isoformat(),
                    "error": error,
                },
            )
        except Exception as e:
            logger.error(f"Failed to update status for {notification_id}: {e}")


# Message Consumer


MAX_RETRIES = 3 
RETRY_DELAYS = [5, 30, 120] # exponential backoff in seconds

async def process_email_message(message: aio_pika.IncomingMessage):
    """Process a single email notification from the queue."""
    async with message.process(request=False):
        try: 
            payload = json.loads(message.body.decode())
            notification_id = payload.get("notification_id")
            user_id = payload.get("user_id")
            template_code = paylaod.get("template_code")
            variables = payload.get("variables", {})
            retry_count = int(message.headers.get("x-retry-count", 0))

            logger.info(f"Processing email: id={notification_id} retry={retry_count}")

            # Idompotency
            cache_key = f"email_processed:{notification_id}"
            if await redis_client.exists(cache_key):
                logger.info(f"Skipping duplicate email: {notification_id}")
                return

            # Fetch user
            user = await fetch_user(user_id)
            if not to_email:
                await update_notifications_status(notifications_id, "failed", "User not found")

                return

            to_email = user.get("email")
            if not to_email:
                await update_notification_status(notification_id, "failed", "No email address for user")
                return

            # Render template
            rendered = await render_template(template_code, {**variables, **{"name": user.get("name", "")}})
            if not rendered:
                await update_notification_status(notification_id, "failed", "Template rendering failed")
                return

            subject = rendered.get("subject") or "Notification"
            body = rendered.get("body", "")

            # Send email
            success = await deliver_email(to_email, subject, body)

            if success:
                await redis_client.set(cache_key, "1", ttl=86400)
                await update_notification_status(notification_id, "delivered")
                logger.info(f"Email delivered: id{notification_id} to={to_email}")
            else:
                # Retry logic
                if retry_count < MAX_RETRIES:
                    delay = RETRY_DELAYS[retry_count]
                    logger.warning(f"Email failed, retrying in {delay}s: id={notification_id}")
                    await asyncio.sleep(delay)
                    # Re-publish with incremented retry count (handled by consumer loop)
                    await update_notification_status(notification_id, "pending", f"Retry {retry_count + 1}")
                    raise Exception("Triggering requeue for entry")
                else:
                    await update_notification_status(notification_id, "failed", "Max retries exceeded")
                    logger.error(f"Email permanently failed after {MAX_RETRIES} retries: id={notification_id}")
        except Exception as e:
            logger.error(f"Error processing email message: {e}", exc_info=True)
            raise   # cause messsage to go to DLQ after max retries

# App

_consumer_task: Optional[asyncio.Task] = None
_connection: Optional[aio_pika.RobustConnection] = None

async def start_consumer():
    global _connection
    logger.info("Connecting Email consumer to RabbitMQ...")
    _connection = await aio_pika.connect_robust(settings.rabbitmq_url, reconnect_interval=5)
    channel = await _connection.channel()
    await channel.set_qos(prefetch_count=5)

    queue = await channel.declare_queue(
        settings.email_queue, durable=True,
        arguments={
            "x-dead-letter-exchange": "notifications.dlx",
            "x-dead-letter-routing-key": settings.failed_queue,
            "x-message-ttl": 86400000,
        },
    )
    await queue.consume(process_email_message)
    logger.info(f"Email consumer listening on {settings.email_queue}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_task
    await redis_client.connect()
    _consumer_task = asyncio.create_task(start_consumer())
    logger.info("Email Service started.")
    yield
    if _consumer_task:
        _consumer_task.cancel()
    if _connection:
        await _connection.close()
    await redis_client.disconnect()

app = FastAPI(
    title="Notification System - Email Service",
    description="Consumes email.queue and delivers email notifications.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health", tags=["Health"])
async def health_check():
    redis_ok = await redis_client.ping()
    rabbit_ok = connection is not None and not _connection.is_closed
    status = "healthy" if (redis_ok and rabbit_ok) else "degraded"
    return {
        "service": "email-service",
        "status": status,
        "checks": {"redis": "up" if redis_ok else "down", "rabbitmq": "up" if rabbit_ok else "down"},
    }