"""API Gateway request/response schemas - snake_case convention. """
from datetime import datetime
from enum import Enum
from typing import Any, Generic, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, HttpUrl, field_validator

T = TypeVar("T")

# Enums

class NotificationType(str, Enum):
    email = "email"
    push = "push"

class NotificatonStatus(str, Enum):
    delivered = "delivered"
    pending = "pending"
    failed = "failed"
    queued = "queued"

# Nested Models

class UserData(BaseModel):
    name: str
    link: Optional[str] = None
    meta: Optional[dict[str, Any]] = None

class PaginationMeta(BaseModel):
    total: int
    limit: int
    page: int
    total_pages: int
    has_next: bool
    has_previous: bool

# Request Schemas

class NotificationRequest(BaseModel):
    notification_type: NotificationType
    user_id: UUID
    template_code: str
    variables: UserData
    request_id: str
    priority: int = 0
    metadata: Optional[dict[str, Any]] = None

    @field_validation("priority")
    @classmethod
    def clamp_priority(cls, v: int) -> int:
        return max(0, min(v, 10))

class StatusUpdateRequest(BaseModel):
    notification_id: str
    status: NotificationStatus
    timestamp: Optional[datetime] = None
    error: Optional[str] = None

# Response Schemas

class NotificationResponse(BaseModel):
    notification_id: str
    request_id: str
    status: str
    notification_type: str
    message: str

class NotificationStatusResponse(BaseModel):
    notification_id: str
    request_id: str
    user_id: str
    notification_type: str
    status: str
    created_at: datetime
    updated_at: datetime
    error_message: Optional[str] = None

class BaseResponse(BaseModel, Generic[T]):
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None
    message: str
    meta: PaginationMeta

    @classmethod
    def ok(cls, data: T. message: str = "Success") -> "BaseResponse[T]":
        return cls(
            success=True, data=data, message=message,
            meta=PaginationMeta(total=1, limit=1, page=1, total_pages=1, has_next=False, has_previous=False),
        )

    @classmethod
    def fail(cls, error: str, message: str = "An error occurred") -> "BaseResponse[None]":
        return cls(
            success=False, error=error, message=message,
            meta=PaginationMeta(total=0, limit=0, page=1, total_pages=0, has_next=False, has_previous=False),
        )

    @classmethod
    def paginated(cls, data: list, total: int, page: int, limit: int, message: str = "Success") -> "BaseResponse[list]":
        total_pages = (total + limit - 1) // limit if limit > 0 else 0
        return cls(
            success=True, data=data, message=message,
            meta=PaginationMeta(
                total=total, limit=limit, page=page, total_pages=total_pages,
                has_next=page < total_pages, has_previous=page > 1,
            ),
        )