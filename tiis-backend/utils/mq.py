"""
Production-ready Redis Pub/Sub wrapper with connection pooling and error handling.
"""

import asyncio
import logging
from typing import Any, Callable, Optional

import orjson
import redis.asyncio as redis
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from utils.config import settings

logger = logging.getLogger(__name__)


class RedisPublisher:
    """Redis publisher for Pub/Sub events with connection pooling and retries."""

    def __init__(self, redis_url: Optional[str] = None) -> None:
        """Initialize Redis publisher.

        Args:
            redis_url: Redis connection URL, defaults to settings.REDIS_URL
        """
        self.redis_url = redis_url or settings.REDIS_URL
        self.client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Establish Redis connection with connection pooling."""
        if self.client is None:
            self.client = redis.from_url(
                self.redis_url,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                decode_responses=False,  # Handle bytes for orjson
            )

    @retry(
        retry=retry_if_exception_type((redis.RedisError, redis.ConnectionError, redis.TimeoutError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
    )
    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        """Publish message to Redis channel with retry logic.

        Args:
            channel: Redis channel name
            message: Message payload dict (will be JSON-serialized)

        Raises:
            redis.RedisError: If publishing fails after retries
        """
        if self.client is None:
            await self.connect()

        # Serialize with orjson for better performance
        message_bytes = orjson.dumps(message)

        await self.client.publish(channel, message_bytes)

    async def close(self) -> None:
        """Close Redis connection and cleanup resources."""
        if self.client:
            await self.client.aclose()
            self.client = None


class RedisSubscriber:
    """Redis subscriber for Pub/Sub events with async message handling."""

    def __init__(self, channels: list[str], redis_url: Optional[str] = None) -> None:
        """Initialize Redis subscriber.

        Args:
            channels: List of channels to subscribe to
            redis_url: Redis connection URL, defaults to settings.REDIS_URL
        """
        self.redis_url = redis_url or settings.REDIS_URL
        self.channels = channels
        self.client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self._stop_event = asyncio.Event()

    async def connect(self) -> None:
        """Establish Redis connection and subscribe to channels."""
        if self.client is None:
            self.client = redis.from_url(
                self.redis_url,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                decode_responses=False,  # Handle bytes for orjson
            )

        self.pubsub = self.client.pubsub()
        await self.pubsub.subscribe(*self.channels)

    async def subscribe(self, handler: Callable[[str, dict[str, Any]], None]) -> None:
        """Subscribe to channels and process messages with handler.

        Args:
            handler: Async callback function(channel, message_dict)
        """
        if self.pubsub is None:
            await self.connect()

        try:
            while not self._stop_event.is_set():
                try:
                    # Poll for messages with timeout to avoid blocking
                    message = await asyncio.wait_for(
                        self.pubsub.get_message(ignore_subscribe_messages=True),
                        timeout=1.0
                    )

                    if message and message["type"] == "message":
                        try:
                            # Decode channel name
                            channel = message["channel"].decode("utf-8")

                            # Deserialize message data with orjson
                            payload = orjson.loads(message["data"])

                            # Call handler
                            await handler(channel, payload)

                        except (orjson.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
                            logger.warning(
                                "Failed to decode message, skipping",
                                extra={"error": str(e), "raw_data": message.get("data")}
                            )
                            continue

                except asyncio.TimeoutError:
                    # Normal timeout, continue polling
                    pass
                except redis.RedisError as e:
                    logger.error("Redis error during subscription", extra={"error": str(e)})
                    await asyncio.sleep(1)  # Brief pause before retry

                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.01)

        except Exception as e:
            logger.error("Subscription loop failed", extra={"error": str(e)})
            raise

    def stop(self) -> None:
        """Signal the subscription loop to stop."""
        self._stop_event.set()

    async def close(self) -> None:
        """Close Redis connection and cleanup resources."""
        try:
            if self.pubsub:
                await self.pubsub.unsubscribe(*self.channels)
                await self.pubsub.aclose()
                self.pubsub = None
        finally:
            if self.client:
                await self.client.aclose()
                self.client = None