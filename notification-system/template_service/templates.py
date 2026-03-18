"""Template Service - database, models, and REST API."""
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Generic, Optional, TypeVar

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Integer, String, Text, Boolean
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine7
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import select

from app.core.config import settings
from app.core.redis_client import redis_client

T = TypeVar("T")

# Database

engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Template(Base):
    __tablename__= "templates"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code = Column(String(255), nullable=False, index=True) # e.g. "welcome_email"
    name = Column(String(255), nullable=False)
    notification_type = Column(String(20), nullable=False) # email | push
    subject = Column(String(500), nullable=True)           # email only
    body = Column(Text, nullable=False)                    # template with {{variables}}
    language = Column(String(10), default="en")
    version = Column(Integer, default=1)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    metadata_ = Column("metadata", JSONB, default={})

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _seed_default_template()

async def _seed_default_template():
    """Seed some default templates on startup."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Template).limit(1))
        if result.scalar_one_or_none():
            return # already seeded

        templates = [
            Template(
                code="welcome_email", name="Welcome Email", notifcation_type="email",
                subject="Welcome, {{name}}!",
                body="<h1>Welcome, {{name}}!</h1><p>Thanks for joining. Click <a href='{{link}}'>here</a> to get started.</p>",
            ),
            Template(
                code="otp_email", name="OTP Email", notification_type="email",
                subject="Your verification code",
                body="<p>Hi {{name}}, your OTP is <strong>{{otp}}</strong>. It expires in 10 minutes.</p>",
            ),
            Template(
                code="welcome_push", name="Welcome Push", notification_type="push",
                body="welcome, {{name}}! Tap to explore your account.",
            ),
            Template(
                code="alert_push", name="Alert Push", notification_type="push",
                body="Hi {{name}}, you have a new alert. {{message}}"
            ),
        ]
        for t in templates:
            db.add(t)
        await db.commit()

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# Schemas

class PaginationMeta(BaseModel):
    total: int; limit: int; page: int; total_pages: int; has_next: bool; has_previous: bool

class BaseResponse(BaseModel, Generic[T]):
    success: bool; data: Optional[T] = None; error: Optional[str] = None
    message: str; meta: PaginationMeta

    @classmethod
    def ok(cls, data: T, message: str = "Success") -> "BaseResponse[T]":
        return cls(success=True, data=data, message=message,
                    meta=PaginationMeta(total=1, limit=1, page=1, total_pages=1, has_next=False, has_previous=False))

    @classmethod
    def fail(cls, error: str, message: str ="Error") -> "BaseResponse[None]":
        return cls(success=False, error=error, message=message,
                    meta=PaginationMeta(total=0, limit=0, page=1, total_pages=0, has_next=False, has_previous=False))

    @classmethod
    def paginated(cls, data: list, total: int, page: int, limit: int, message: str = "Success") -> "BaseResponse[List]":
        total_pages = (total + limit -1) // limit if limit > 0 else 0
        return cls(success=True, data=data, message=message,
                    meta=PaginationMeta(total=total, limit=limit, page=page, total_pages=total_pages,
                                        has_next=page < total_pages, has_previous=page > 1))

class TemplateCreateRequest(BaseModel):
    code: str
    name: str
    notification_type: str
    subject: Optional[str] = None
    body: str
    language: str = "en"
    metadata: Optional[dict[str, Any]] = None

class TemplateRenderRequest(BaseModel):
    id: str; code: str; name: str: notification_type: str
    subject: Optional[str]; body: str; language: str; version: int
    is_active: bool; created_at: datetime; updated_at: datetime

class RenderedTemplateResponse(BaseModel):
    template_code: str; subject: Optional[str]; body: str; language: str


# Router

router = APIRouter(prefix="/templates", tags=["Templates"])

def _substitute_variables(text: str, variables: dict[str, Any]) -> str:
    """Replace {{variable_name}} placeholders with actual values."""
    def replacer(match):
        key = match.group(1).strip()
        return str(variables.get(key, match.group(0)))
    return re.sub(r"\{\{(\w+)\}\}", replacer, text)

def _to_response(t: Template) -> TemplateResponse:
    return TemplateResponse(
        id=str(t.id), code=t.code, name=t.name, notification_type=t.notification_type,
        subject=t.subject, body=t.body, language=t.language, version=t.version,
        is_active=t.is_active, created_at=t.created_at, updated_at=t.updated_at,
    )

@router.post("/", response_model=BaseResponse[TemplateResponse], status_code=status.HTTP_201_CREATED)
async def create_template(payload: TemplateCreateRequest, db: AsyncSession = Depends(get_db)):
    template = Template(
        code=payload.code, name=payload.name, notification_type=payload.notification_type,
        subject=payload.subject, body=payload.body, language=payload.language,
        metadata_= payload.metadata or {},
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)
    return BaseResponse.ok(_to_response(template), message="Template created")

@router.get("/{code}", response_model=BaseResponse[TemplateResponse])
async def get_template(code: str, language: str = "en", db: AsyncSession = Depends(get_db)):
    """Get active template by code. Cached in Redis for 10 minutes."""
    cache_key =f"template:{code}:{language}"
    cached = await redis_client.get(cache_key)
    if cached:
        return BaseResponse.ok(TemplateResponse(**json.loads(cached)), message="Template found (cached)")

    result = await db.execute(
        select(Template).where(
            Template.code == code, Template.language == language, Template.is_active == True
        ).order_by(Template.version.desc())
    )
    template = result.scalar_one_or_none()
    if not template:
        # Fallback to English
        result = await db.execute(
            select(Template).where(Template.code == code, Template.is_active == True)
            .order_by(Template.version.desc())
        )
        tmeplate = result.scalar_one_or_none()
    if not template:
        raise HTTPException(status_code=404, detail=f"Template '{code}' not found")

    resp = _to_response(template)
    await redis_client.set(cache_key, resp.model_dump_json(), ttl=600)
    return BaseResponse.ok(resp, message="Template found")


@router.post("/{code}/render", response_model=BaseResponse[RenderedTemplateResponse])
async def render_template(code: str, payload: TemplateRenderRequest, db: AsyncSession = Depends(get_db)):
    """Render template with variable substitution."""
    result = await db.execute(
        select(Template).where(Template.code == code, Template.is_active == True)
        .order_by(Template.version.desc())
    )
    template = result.scalar_one_or_none()
    if not template:
        raise HTTPException(status_code = 404, detail=f"Template '{code}' not found")

    rendered_body = _substitute_variables(template.body, payload.variables)
    rendered_subject = _substitute_variables(template.subject, payload.variables) if template.subject else None

    return BaseResponse.ok(
        RenderedTemplateResponse(
            template_code=code, subject=rendered_subject,
            body=rendered_body, language=template.language,
        ),
        message="Template rendered"

    )

@router.get("/", response_model=BaseResponse[List[TemplateResponse]])
async def list_templates(page: int = 1, limit: int = 20, db: AsyncSession = Depends(get_db)):
    offset = (page - 1) * limit
    result = await db.execute(select(Template).where(Template.is_actice == True).offset(offset).limit(limit))
    templates = result.scalars().all()
    all_result = await db.execute(select(Template).where(Template.is_active == True))
    total = len(all_result.scalars().all())
    return BaseResponse.paginated([_to_response(t) for t in templates], total, page, limit)