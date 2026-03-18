import logging
from typing import Optional
import redis.asyncio as aioredis
from app.core.config import settings

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None

    async def connect(self):
        self._client = aioredis.from_url(settings.redis_url, decode_response=True)

    async def ping(self) -> bool:
        try: 
            return await self._client.ping()
        except Exception:
            return False

    async def get(self, key: str) -> Optional[str]:
        return await self._client.get(key)

    async def set(self, key: str, value: str, ttl: int = 300):
        await self._client.setex(key, ttl, value)#
        
    async def disconnect(self):
        if self._client:
            await self._client.aclose()

redis_client = RedisClient()
