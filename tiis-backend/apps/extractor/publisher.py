"""
Event Publisher for Extractor Service

Publishes file creation events to Redis Pub/Sub after successful extraction.

Features:
- Redis Pub/Sub integration via production wrapper
- Automatic connection management and retries
- JSON message serialization
- Structured logging

Usage:
    from apps.extractor.publisher import publish_extraction_event

    await publish_extraction_event("/data/raw_users/records_20250101_120000.jsonl")
"""

import logging
from datetime import datetime, timezone

from utils.config import settings
from utils.mq import RedisPublisher

logger = logging.getLogger(__name__)


async def publish_extraction_event(output_file: str) -> None:
    """
    Publish raw file creation event to Redis channel.

    Args:
        output_file: Absolute path to created JSONL file

    Raises:
        redis.RedisError: If publishing fails
    """
    publisher = RedisPublisher()

    try:
        # Create event message
        message = {
            "type": "raw_created",
            "path": output_file,
            "ts": datetime.now(timezone.utc).isoformat(),
            "channel": settings.REDIS_CHANNEL_RAW,
        }

        # Publish to channel
        await publisher.publish(settings.REDIS_CHANNEL_RAW, message)

        logger.info(
            "Published extraction event",
            extra={
                "channel": settings.REDIS_CHANNEL_RAW,
                "file_path": output_file,
                "message_type": "raw_created",
            },
        )

    except Exception as e:
        logger.error(
            "Failed to publish event",
            extra={
                "channel": settings.REDIS_CHANNEL_RAW,
                "file_path": output_file,
                "error": str(e),
            },
        )
        raise

    finally:
        # Cleanup connection
        await publisher.close()