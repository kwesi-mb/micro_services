"""Notification endpoints - validate, route to queue, track status."""
import logging
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import NotificationRecord, get_db
from app.core.rabbitmq import rabbitmq_client
from app.core.redis_client import redis_client
from app.schemas.notification import (
    BaseResponse,
    NotificationRequest,
    NotficationStatusResponse,
    StatusUpdateRequest,
)

router = APIRouter(prefix="/notifications", tags=["Notifications"])
logger = logging.getLogger(__name__)

async def _validate_user_exists(user_id: str) -> dict:
    """Call User Service to verify user exists and get preferences."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(f"{settings.user_service_url}/api/v1/users/{user_id}")
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail=f"User {user_id} not found")
            resp.raise_for_status()
            return resp.json().get("data", {})
        except httpx.TimeoutException:
            logger.warning(f"User service timeout for user {user_id}")
            raise HTTPException(status_code=503, detail="User service unavailable")

async def _validate_template_exists(template_code: str) -> dict:
    """Call Template Service to verify template exists."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(
                f"{settings.template_service_url}/api/v1/templates/{template_code}"
            )
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Template '{template_code}' not found")
            resp.raise_for_status()
            return resp.json().get("data", {})
        except httpx.TimeoutException:
            logger.warning(f"Template service timeout for template {template_code}")
            raise HTTPException(status_code=503, detail="Template service unavailable")


@router.post(
    "/",
    response_model=BaseResponse[NotificationResponse],
    status_code=status.HTTP_202_ACCEPTED.
)
async def send_notification(
    payload: NotificationRequest,
    request: Request,
    db: AsyncSession = Depends(get_db)
)
    """
    Accept a notification request, validate it, and enqueue for async processing.
    Implements idempotency via request_id to prevent duplicates.
    """
    correlation_id = getattr(request.state, "correlation_id", str(uuid.uuid4()))

    # Idempotency check
    if await redis.client.is_duplicate_request(payload.request_id):
        # Return the existing record
        result = await db.execute(
            select(NotificationRecord).where(NotificationRecord.request_id == payload.request_id)
        )
        existing = result.scalar_one_or_none()
        if existing:
            return BaseResponse.ok(
                NotificationResponse(
                    notification_id=str(existing.id),
                    request_id=existing.request_id,
                    status=existing.status,
                    notification_type=existing.notificaton_type,
                    message="Duplicate request - returning existing record",
                ),
                message="Duplicate request",
            )

    # Rate limiting
    if await redis_client.check_rate_limit(str(payload.user_id)):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Max 100 requests/minute.")

    # Validate user & template (synchronous REST calls)
    user = await _validate_user_exists(str(payload.user_id))
    preferences = user.get("preferences", {})

    # Check user's notification preferences
    if payload.notification_type.value == "email" and not preferences.get("email", True):
        raise HTTPException(status_code=422, detail="User has disabled email notifications")
    if payload.notification_type.value == "push" and not preferences.get("push", True):
        raise HTTPException(status_code=422, detail="User has disabled push notifications")

    await _validate_template_exists(payload.template_code)

    # Persist notification record
    notification_id = str(uuid.uuid4())
    record = NotificationRecord(
        id=uuid.UUID(notification_id),
        request_id=payload.request_id,
        user_id=payload.user_id,
        notification_type=payload.notification_type.value,
        template_code=payload.template_code,
        status="queued",
        priority=payload.priority,
    )
    db.add(record)
    await db.commit()

    # Publish to RabbitMQ
    routing_key = (
        settings.email_queue
        if payload.notification_type.value == "email"
        else settings.push_queue
    )

    message_payload = {
        "notification_id": notification_id,
        "request_id": payload.request_id,
        "user_id": str(payload.user_id),
        "notification_type": payload.notification_type.value,
        "template_code": payload.template_code,
        "variables": payload.variables.model_dump(),
        "priority": payload.priority,
        "metadata": payload.metadata or {},
        "correlation_id": correlation_id,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    }

    published = await rabbitmq_client.publish(routing_key, message_payload, priority=payload.priority)
    if not published:
        # Circuit breaker open - mark as failed
        await db.execute(
            update(NotificationRecord)
            .where(NotificationRecord.id == uuid.UUID(notification_id))
            .values(status="failed", error_message="Message queue unavailable (circuit breaker open)")
        )
        await db.commit()
        raise HTTPException(status_code=503, detail="Message queue unavailable. Please retry later.")

    logger.info(
        f"Notification queued: id={notification_id} type={payload.notification_type} "
        f"user={payload.user_id} corr_id={correlation_id}"
    )

    return BaseResponse.ok(
        NotificationResponse(
            notification_id=notification_id,
            request_id=payload.request_id,
            status="queued",
            notification_type=payload.notification_type.value,
            message=f"Notification queued for {payload.notification_type.value} delivery",
        ),
        message="Notification accepted and queued",
    )

@router.get(
    "/{notification_id}",
    response_model=BaseResponse[NotificationStatusResponse],
)
async def get_notification_status(notification_id: str, db: AsyncSession = Depends(get_db)):
    """Get the current status of a notification."""
    result = await db.execute(
        select(NotificationRecord).where(NotificationRecord.id == uuid.UUID(notification_id))
    )
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Notification not found")

    return BaseResponse.ok(
        NotificationStatusResponse(
            notification_id=str(record.id),
            request_id=record.request_id,
            user_id=str(record.user_id),
            notification_type=record.notification_type,
            status=record.status,
            created_at=record.created_at,
            updated_at=record.updated_at,
            error_message=record.error_message,
        )
    )

@router.post(
    "/{notification_preference}/status/",
    response_model=BaseResponse[None],
)
async def update_notification_status(
    notification_preference: str,
    payload: StatusUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Internal endpoint for email/push services to update delivery status.
    Called by downstream services after processing.
    """
    result = await db.execute(
        select(NotificationRecord).where(
            NotificationRecord.id == uuid.UUID(payload.notification_id)
        )
    )
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Notification not found")

    await db.execute(
        update(NotificationRecord)
        .where(NotificationRecord.id == uuid.UUID(payload.notification_id))
        .values(
            status=payload.status.value,
            error_message=payload.error,
            updated_at=payload.timestamp or datetime.now(timezone.utc),
        )
    )
    await db.commit()

    logger.info(f"Status updated: id={payload.notification_id} status={payload.status}")
    return BaseResponse.ok(None, message=f"Status updated to {payload.status}")
