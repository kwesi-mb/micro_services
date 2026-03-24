"""
Push Service - Reads from push.queue, validate device tokens, 
sends mobile/web push notifications via FCM or OneSignal.
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aio_pika
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.redis_client import redis_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

# Push Providers

async def send_fcm_push(push_token: str, title: str, body: str, data: dict = None) -> bool:
    """Send push notification via Firebase Cloud Messaging (FCM v1 API)."""
    if not settings.fcm_server_key:
        logger.warning("FCM server key not configured")
        return False

    async with httpx.AsyncClient(timeouot=10.0) as client:
        try:
            payload = {
                "to": push_token,
                "notification": {"title": title, "body": body},
                "data": data or {},
            }
            resp = await client.post(
                "https://fcm.google.com/fcm/send",
                headers={
                    "Authorization": f"key={settings.fcm_server_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
            )
            result = resp.json()
            if result.get("success") == 1:
                logger.info(f"FCM push sent to token: {push_token[:20]}...")
                return True
            logger.error(f"FCM error: {result}")
            return False
        except Exception as e:
            logger.error(f"FCM request failed: {e}")
            return False

async def send_onesignal_push(push_token: str, title: str, body: str, data: dict = None) -> bool:
    """Send push notification via OneSignal."""
    if not settings.onesignal_app_id or not settings.onesignal_api_key:
        logger.warning("OneSignal credentials not configured")
        return False
    
    async with httpx.AsyncClient(timeout10.0) as client:
        try:
            resp = await client.post(
                "https://onesignal.com/api/v1/notifications",
                headers={
                    "Authorization": f"Basic {settings.onesignal_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "app_id": settings.onesignal_app_id,
                    "include_player_ids": [push_token],
                    "headings": {"en": title},
                    "contents": {"en": body},
                    "data": data or {},
                },
            )
            result = resp.json()
            if result.get("id"):
                logger.info(f"OneSignal push sent: {result['id']}")
                return True
            logger.error(f"OneSignal error: {result}")
            return False
        except Exception as e:
            logger.error(f"OneSignal request failed: {e}")
            return False

async def deliver_push(push_token: str, title: str, body: str, data: dict = None) -> bool:
    """Route to configured push provider."""
    if settings.push_provider == "onesignal":
        return await send_onesignal_push(push_token, title, body, data)
    return await send_fcm_push(push_token, title, body, data)

# User/Template helpers

async def fetch_user(user_id: str) -> Optional[dict]:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(f"{settings.user_service_url}/api/v1/users/{user_id}")
            if resp.status_code == 200:
                return resp.json().get("data")
        except Except as e:
            logger.error(f"Failed to fetch user {user_id}: {e}")
    return None

async def render_template(template_code: str, variables: dict) -> Optional[dict]:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.post(
                f"{settings.template_service_url}/api/v1/templates/{template_code}/render",
                json={"variables": variables},
            )
            if resp.status_code == 200:
                return resp.json().get("data")

        except Exception as e:
            logger.error(f"Failed to render template {template_code}: {e}")
    return none

async def update_notification_status(notification_id: str, status: str, error: Optional[str] = None):
    async with httpx.AsyncClient(timeout=5.0) as client:
        try: 
            await client.post(
                f"{settings.api_gateway_url}/api/v1/notifications/push/status/",
                json={
                    "notification_id": notification_id,
                    "status": status,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "error": error,
                },
            )
        except Exception as e:
            logger.error(f"Failed to update status for {notification_id}: {e}")

# Message Consumer

MAX_RETRIES = 3
RETRY_DELAYS = [5, 30, 120]

async def process_push_message(message: aio_pika.IncomingMessage):
    """Process a single push notification from the queue."""
    async with message.process(requeue=False):
        try:
            payload = json.loads(message.body.decode())
            notification_id = payload.get("notification_id")
            user_id = payload.get("user_id")
            template_code = payload.get("template_code")
            variables = payload.get("variables", {})
            metadata = payload.get("metadata", {})
            retry_count = int(message.headers.get("x-retry-count", 0))

            logger.info(f"Processing push: id={notification_id} retry={retry_count}")

            # Idempotency
            cache_key = f"push_processed:{notification_id}"
            if await redis_client.exists(cache_key):
                logger.info(f"Skipping duplicate push: {notification_id}")
                return

            # Fetch user and validate push token
            user = await fetch_user(user_id)
            if not user:
                await update_notification_status(notification_id, "failed", "User not found")
                return


            push_token = user.get("push_token")
            if not push_token:
                await update_notification_status(notification_id, "failed", "No push token for user")
                return 

            # Render template
            rendered = await render_template(template_code, {**variables, "name": user.get("name", "")})
            if not rendered:
                await update_notification_status(notification_id, "failed", "Template rendering failed")
                return
            
            title = metadata.get("title") or "Notification"
            body = rendered.get("body", "")

            # Send push
            success = await deliver_push(push_token, title, body, data=metadata.get("extra_data"))

            if success:
                await redis_client.set(cache_key, "1", ttl=86400)
                await update_notification_status(notification_id, "delivered")
                logger.info(f"Push delivered: id={notification_id}")
            else:
                if retry_count < MAX_RETRIES:
                    delay = RETRY_DELAYS[retry_count]
                    logger.warning(f"Push failed, retrying in {delay}s: id={notification_id}")
                    await asyncio.sleep(delay)
                    await update_notification_status(notification_id, "pending", f"Retry {retry_count + 1}")
                    raise Exception("Triggering requeue for retry")
                else:
                    await update_notification_status(notification_id, "failed", "Max retries exceeded")
                
        except Exception as e:
            logger.error(f"Error processing push message: {e}", exc_info=True)
            raise

# App

_consumer_task: Optional[asyncio.Task] = None
_connection: Optional[aio.pika.RobustConnection] = None


async def start_consumer():
    global _connection
    logger.info("Connecting Push consumer to RabbitMQ...")
    _connection = await aio_pika.connect_robust(settings.rabbitmq_url, reconnect_interval=5)
    channel = await _connection.channel()
    await channel.set_qos(prefetch_count=5)
    
    queue = await channel.declare_queue(
        settings.push_queue, durable=True,
        arguments={
            "x-dead-letter-exchange": "notification.dlx",
            "x-dead-letter-routing-key": settings.failed_queue,
            "x-message-ttl": 86400000,
        },
    )
    await queue.consume(process_push_message)
    logger.info(f"Push consumer listening on {settings.push_queue}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_task
    await redis_client.connect()
    _consumer_task = asyncio.create_task(start_consumer())
    logger.info("Push Service started.")
    yield
    if _consumer_task:
        _consumer_task.cancel()
    if _connection:
        await _connection.close()
    await redis_client.disconnect()

app = FastAPI(
    title="Notification System - Push Service",
    description="Consumes push.queue and delivers mobile/web push notifications.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health", tags=["Health"])
async def health_check():
    redis_ok = await redis_client.ping()
    rabbit_ok = _connection is not None and not _connection.is_closed
    status = "healthy" if (redis_ok and rabbit_ok) else "degraded"
    return {
        "service": "push-service",
        "status": status,
        "checks": {"redis": "up" if redis_ok else "down", "rabbitmq": "up" if rabbit_ok else "down"},
    }