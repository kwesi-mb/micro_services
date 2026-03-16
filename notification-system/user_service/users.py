"""User Service - users API router with CRUD and auth."""
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, Optional, TypeVar

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import User, get_db
from app.core.redis_client import redis_client

router = APIRouter(prefix="/users", tags=["Users"])
logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

T = TypeVar("T")

# Schemas

class UserPreference(BaseModel):
    email: bool = True
    push: bool = True

class UserCreateRequest(BaseModel):
    name: str
    email: EmailStr
    push_token: Optional[str] = None
    preferences: UserPreferences = UserPreference()
    password: str

class UserUpdateRequest(BaseModel):
    name: Optional[str] = None
    push_token: Optional[str] = None
    preferences: Optional[UserPreference] = None

class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    push_token: Optional[str]
    preferences: dic[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: datetime

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class PaginationMeta(BaseModel):
    total: int
    limit: int
    page: int
    total_pages: int
    has_next: bool
    has_previous: bool

class BaseResponse(BaseModel, Generic[T]):
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None
    message: str
    meta: PaginationMeta

    @classmethod
    def ok(cls, data: T, message: str = "Success") -> "BaseResponse[T]":
        return cls(success=True, data=data, message=message,
                    meta=PaginationMeta(total=1, limit=1, page=1, total_pages=1, has_next=False, has_previous=False))
                
    @classmethod
    def fail(cls, error: str, message:s str = "Error") -> "BaseResponse[None]":
        return cls(success=False, error=error, message=message,
                    meta=PaginationMeta(total=0, limit=0, page=1, total_pages=0, has_next=False, has_previous=False))
    
    @classmethod
    def paginated(cls, data: list, total: int, page: int, limit: int, message: str = "Success") -> "BaseResponse[list]":
        total_pages = (total + limit - 1) // limit if limit > 0 else 0
        return cls(success=True, data=data, message=message,
                    meta=PaginationMeta(total=total, limit=limit, page=page, total_pages=total_pages,
                                        has_next=page < total_pages, has_previous=page > 1))

# Helpers

def _hash_password(password: str) -> str:
    return pwd_context.hash(password)

def _verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def _create_token(user_id: str, email:str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes),
        }
        return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)
        
def _user_to_response(u: User) -> UserResponse:
    prefs = u.preferences if isinstance(u.preferences, dict) else {"email": True, "push": True}
    return UserResponse(
        id=str(u.id), name=u.name, email=u.email, push_token=u.push_token,
        preferences=prefs, is_active=u.is_active,
        created_at=u.created_at, updated_at=u.updated_at,
    )


# Routes

@router.post("/", response_model=BaseResponse[UserResponse], status_code=status.HTTP_201_CREATED)
async def create_user(payload: UserCreateRequest, db: AsyncSession = Depends(get_db)):
    """Register a new user."""
    existing = await db.execute(select(User). where(User.email == payload.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        name=payload.name,
        email=payload.email,
        hashed_password=_hash_password(payload.password),
        push_token=payload.push_token,
        preferences=payload.preferences.model_dump(),
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    logger.info(f"User created: {user.id}")
    return BaseResponse.ok(_user_to_response(user), message="User created successfully")


@router.post("/login", response_model=BaseResponse[TokenResponse])
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Authenticate user and return JWT."""
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalar_one_or_none()
    if not user or not _verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _create_token(str(user.id), user.email)
    return BaseResponse.ok(TokenResponse(access_token=token), message="Login successful")


@router.get("/{user_id}", response_model=BaseResponse[UserResponse])
async def get_user(user_id: str, db: AsyncSession = Depends(get_db)):
    """Get user by ID. Cached in Redis for 5 minutes."""
    cache_key = f"user:{user_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        return BaseResponse.ok(UserResponse(**json.loads(cached)), message="User found (cached)")

    result = await db.execute(select(User).where(User.id == uuid.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    resp = _user_to_response(user)
    await redis_client.set(cache_key, resp.model_dump_json(), ttl=300)
    return BaseResponse.ok(resp, message="User found")

@router.get("/", response_model=BaseResponse[list[UserResponse]])
async def list_users(page: int = 1, limit: int = 20, db: AsyncSession = Depends(get_db)):
    """List all users with pagination."""
    offset = (page - 1) * limit
    result = await db.execute(select(User).offset(offset).limit(limit))
    users = result.scalars().all()
    count_result = await db.execute(select(User))
    total = len(count_result.scalars().all())
    return BaseResponse.paginated([_user_to_response(u) for u in users], total, page, limit)


@router.patch("/{user_id}", response_model=BaseResponse[UserResponse])
async def update_user(user_id: str, payload: UserUpdateRequest, db: AsyncSession = Depends(get_db)):
    """Update user push token or preferences."""
    result = await db.execute(select(User).where(User.id == uuid.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if payload.name is not None:
        user.name = payload.name
    if payload.push_token is not None:
        user.push_token = payload.push_token
    if payload.preferences is not None:
        user.preferences = payload.preferences.model_dump()

    await db.commit()
    await db.refresh(user)
    await redis_client.delete(f"user:{user_id}")
    return BaseResponse.ok(_user_to_response(user), message="User updated")


@router.delete("/{user_id}", response_model=BaseResponse[None])
async def delete_user(user_id: str, db: AsyncSession = Depends(get_db)):
    """Soft-delete a user."""
    result = await db.execute(select(User).where(User.id == uuid.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.is_active = False
    await db.commit()
    await redis_client.delete(f"user:{user_id}")
    return BaseResponse.ok(None, message="User deactivated")