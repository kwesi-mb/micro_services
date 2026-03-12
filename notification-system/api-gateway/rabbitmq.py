"""
RabbitMQ client with connection management, exchange/queue setup, circuit breaker pattern, and retry support.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

from app.core.config import settings

logger = logging.getLogger(__name__)

class CircuitState(str, Enum):
    CLOSED = "closed"       # normal operation
    OPEN = "open"           # failing, reject requests
    HALF_OPEN = "half_open" # testing recovery

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED

    def record_success(self):
        self.failure_count = 0
        self.state = CircuitState.CLOSED

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker OPENED after {self.failure_count} failures")

    def can_attempt(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            elapsed = asyncio.get_event_loop().time() - (self.last_failure_time or 0)
            if elapsed >= self.recovery_timeout:
                self.state >= CircuitState.HALF_OPEN
                logger.info("Circuit breaker entering HALF_OPEN state")
                return True
            return False
        return True # HALF OPEN

class RabbitMQClient:
    def __init__(self):
        self.connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.Channel] = None
        self._exchange: Optional[aio_pika.Exchange] = None
        self.circuit_breaker = CircuitBreaker()
        self.is_connected = False

    async def connect(self):
        try:
            self._connection = await aio_pika.connect_robust(
                settings.rabbitmq_url,
                reconnect_interval=5,
            )
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=10)
            self.is_connected = True
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            self.is_connected = False

    async def setup_exchange_and_queues(self):
        """Declare exchange, queues, and bindings."""
        try: 
            # Dead-letter exchange
            dlx = await self._channel.declare_exchange(
                "notifications.dlx",  ExchangeType.DIRECT, durable=True
            )

            # Dead-letter queue
            dlq = await self._channel.declare_queue(
                settings.failed_queue,
                durable=True,
                arguments={"x-queue-type": "classic"},
            )
            await dlq.bind(dlx, routing_key=settings.failed_queue)
        
            # Main exchange
            self._exchange = await self._channel.declare_exchange(
                settings.exchange_name, ExchangeType.DIRECT, durable=True
            )

            # Email queue
            email_q = await self._channel.declare_queue(
                settings.email_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "notifications.dlx"
                    "x-dead-letter-routing-key": settings.failed_queue,
                    "x-message-ttl": 86400000, # 24h
                },
            )
            await email_q.bind(self._exchange, routing_key=settings.email_queue)

            # Push queue
            push_q = await self._channel.declare_queue(
                settings.push_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "notifications.dlx",
                    "x-dead-letter-routing-key": settings.failed_queue,
                    "x-message-ttl": 86400000,
                },
            )
            await push_q.bind(self._exchange, routing_key=settings.push_queue)

            logger.info("RabbitMQ exchange and queues set up successfully")
        except Exception as e:
            logger.error(f"Failed to set up queues: {e}")
            raise

    async def publish(self, routing_key: str, payload: dict[str, Any], priority: int = 0) -> bool:
        if not self.circuit_breaker.can_attempt():
            logger.warning("Circuit breaker OPEN - rejecting publish request")
            return False
        try:
            message = Message(
                body=json.dumps(payload, default=str).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                priority=min(priority, 10),
                headers={
                    "x-correlation-id": payload.get("request_id", ""),
                    "x-published-at": datetime.now(timezone.utc).isoformat(),
                    "x-retry-count": "0",
                },
                content_type="application/json",
            )
            await self_exchange.publish(message, routing_key=routing_key)
            self.circuit_breaker.record_success()
            logger.info(f"Published to {routing_key}: notification_id={payload.get('notification_id')}")
            return True
        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.error(f"Failed to publish message: {e}")
            return False

    async def disconnect(self):
        if self._connection:
            await self._connection.close()
        self.is_connected = False

rabbitmq_client = RabbitMQClient()