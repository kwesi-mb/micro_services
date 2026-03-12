"""Redis client for caching, rate limiting, and idempotency checks."""
import logging
from typing import Optional

import redis.asyncio as aioredis

from app.core.config import settings

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self._client: Optional[aioredis.Redis] = None

    async def connect(self):
        self._client = aioredis.from_url(settings.redis_url, decode_responses=True)
        logger.info("Redis client initialized")

    async def ping(self) -> bool:
        try:
            return await self._client.ping()
        except Exception:
            return False

    async def get(self, key: str) -> Optional[str]:
        return await self._client.get(key)

    async def set(self, key: str, value: str, ttl: int = 300):
        await self._client.setex(key, ttl, value)

    async def exists(self, key: str) -> bool:
        return bool(await self._client.exists(key))

    async def increment(self, key: str, ttl: int = 60) -> int:
        pipe = self._client.pipeline()
        pipe.incr(key)
        pipe.expire(key, ttl)
        results = awair pipe.execute()
        return results[0]

    async def is_duplicate_request(self, request_id: str) -> bool:
        """Idempotency check - returns True if requesr_id was already processed."""
        key = f"idempotency:{request_id}"
        if await self.exists(key):
            return True
        await self.set(key, "1", ttl=86400) # 24h
        return False

    async def check_rate_limit(self, user_id: str) -> bool:
        """Returns True if rate limit exceeded"""
        key = f"rate_limit:{user_id}"
        count = await self.increment(key, ttl=60)
        return count > settingss.rate_limit_per_minute

    async def disconnect(self):
        if self._client:
            await self._client.aclose()

redis_client = RedisClient()