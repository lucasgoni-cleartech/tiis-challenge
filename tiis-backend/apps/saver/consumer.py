"""
Saver Consumer - Database Persistence for Processed and DLQ Files

Consumes processed and DLQ file creation events from Redis Pub/Sub and saves
the file contents to SQLite database. This is the entry point for Phase 3
database persistence of the ETL pipeline.

Features:
- Redis Pub/Sub subscription for processed and DLQ events
- Stream processing of JSONL files
- SQLite database persistence with timestamps
- Graceful shutdown handling
- Structured logging

Usage:
    # Consumer mode (default)
    python -m apps.saver.consumer

    # For development/testing
    RUN_ONCE=true python -m apps.saver.consumer
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import orjson
from pydantic import ValidationError

from utils.config import settings
from utils.db import get_conn, init_schema
from utils.mq import RedisSubscriber
from utils.schemas import RedisEvent
from utils.sftp import upload_file

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class SaverConsumer:
    """
    Consumer for processing processed and DLQ file events from Redis Pub/Sub.

    Handles:
    - Redis subscription management for multiple channels
    - Database persistence of file contents
    - Signal handling for graceful shutdown
    """

    def __init__(self, run_once: bool = False) -> None:
        """
        Initialize saver consumer.

        Args:
            run_once: If True, process one event and exit (for testing)
        """
        self.run_once = run_once
        self.subscriber: RedisSubscriber | None = None
        self.shutdown_event = asyncio.Event()
        self._processed_count = 0

        logger.info(
            "SaverConsumer initialized (run_once=%s, channels=[%s, %s, %s])",
            run_once,
            settings.REDIS_CHANNEL_RAW,
            settings.REDIS_CHANNEL_PROCESSED,
            settings.REDIS_CHANNEL_DLQ,
        )

    async def handle_message(self, channel: str, message: Dict[str, Any]) -> None:
        """
        Handle incoming Redis Pub/Sub message.

        Routes messages to appropriate handlers based on event type.

        Args:
            channel: Redis channel name
            message: Decoded message payload

        Raises:
            Exception: If message processing fails critically
        """
        try:
            # Validate event using Pydantic schema
            try:
                event = RedisEvent(**message)
            except ValidationError as e:
                logger.warning("Invalid event payload (channel=%s): %s", channel, str(e))
                return

            # Route to appropriate handler
            if event.type == "raw_created":
                await self.handle_raw_created(event)
            elif event.type == "processed_created":
                await self.handle_processed_created(event)
            elif event.type == "dlq_created":
                await self.handle_dlq_created(event)
            else:
                logger.debug("Ignoring event type=%s (channel=%s)", event.type, channel)
                return

            self._processed_count += 1

            # Signal shutdown if run_once mode
            if self.run_once:
                logger.info("RUN_ONCE mode: signaling shutdown after processing event")
                self.shutdown_event.set()

        except Exception as e:
            logger.error(
                "Failed to process message (channel=%s): %s",
                channel, str(e),
                exc_info=True,
            )
            raise

    async def handle_raw_created(self, event: RedisEvent) -> None:
        """
        Process a raw_created event by saving raw records to database and uploading to SFTP.

        Args:
            event: Validated Redis event containing file path
        """
        file_path = Path(event.path)
        start_time = time.time()

        logger.info("Processing raw file (DB + SFTP): path=%s", str(file_path))

        # Check if file exists and is readable
        if not file_path.exists():
            logger.warning("Raw file not found: path=%s", str(file_path))
            return

        if not file_path.is_file():
            logger.warning("Raw path is not a file: path=%s", str(file_path))
            return

        # Phase 1: Database persistence
        total_lines = 0
        inserted_count = 0
        current_ts = datetime.now(timezone.utc).isoformat()

        try:
            with get_conn() as conn:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue

                        total_lines += 1

                        try:
                            # Parse JSON with orjson fallback to json
                            try:
                                record = orjson.loads(line)
                            except orjson.JSONDecodeError:
                                record = json.loads(line)

                            # Extract id if present
                            user_id = record.get("id")

                            # Insert into raw_users table
                            conn.execute(
                                "INSERT INTO raw_users (id, inserted_at, data) VALUES (?, ?, ?)",
                                (user_id, current_ts, line)
                            )
                            inserted_count += 1

                        except (orjson.JSONDecodeError, json.JSONDecodeError) as e:
                            logger.debug("Raw insert error at line=%d: JSON parse failed: %s", line_num, str(e))
                        except Exception as e:
                            logger.debug("Raw insert error at line=%d: Database insert failed: %s", line_num, str(e))

                conn.commit()

        except Exception as e:
            logger.warning("Failed to process raw file for database: path=%s, error=%s", str(file_path), str(e))
            return

        # Calculate processing time
        elapsed_time = time.time() - start_time

        # Log database processing summary
        logger.info(
            "Saved raw users: file=%s, lines=%d, inserted=%d, elapsed=%.3fs",
            str(file_path), total_lines, inserted_count, elapsed_time
        )

        # Phase 2: SFTP upload (unchanged)
        try:
            remote_dir = f"{settings.SFTP_REMOTE_BASE}/raw_users"
            upload_file(str(file_path), remote_dir)
            logger.info("Raw file uploaded to SFTP: local=%s, remote_dir=%s", str(file_path), remote_dir)
        except Exception as e:
            logger.error("Failed to upload raw file to SFTP: local=%s, error=%s", str(file_path), str(e))
            # Continue without aborting - SFTP failure does not invalidate DB persistence

    async def handle_processed_created(self, event: RedisEvent) -> None:
        """
        Process a processed_created event by saving processed user records to database.

        Args:
            event: Validated Redis event containing file path
        """
        file_path = Path(event.path)
        start_time = time.time()

        logger.info("Processing processed file: path=%s", str(file_path))

        # Check if file exists and is readable
        if not file_path.exists():
            logger.warning("Processed file not found: path=%s", str(file_path))
            return

        if not file_path.is_file():
            logger.warning("Processed path is not a file: path=%s", str(file_path))
            return

        # Process file line by line and save to database
        total_lines = 0
        inserted_count = 0
        current_ts = datetime.now(timezone.utc).isoformat()

        try:
            with get_conn() as conn:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue

                        total_lines += 1

                        try:
                            # Parse JSON with orjson fallback to json
                            try:
                                record = orjson.loads(line)
                            except orjson.JSONDecodeError:
                                record = json.loads(line)

                            # Extract fields for database
                            user_id = record.get("id")
                            department_code = record.get("company", {}).get("department_code")

                            # Insert into processed_users table
                            conn.execute(
                                "INSERT INTO processed_users (id, inserted_at, data, department_code) VALUES (?, ?, ?, ?)",
                                (user_id, current_ts, line, department_code)
                            )
                            inserted_count += 1

                        except (orjson.JSONDecodeError, json.JSONDecodeError) as e:
                            logger.debug("JSON parse error at line=%d: %s", line_num, str(e))
                        except Exception as e:
                            logger.debug("Database insert error at line=%d: %s", line_num, str(e))

                conn.commit()

        except Exception as e:
            logger.warning("Failed to process processed file: path=%s, error=%s", str(file_path), str(e))
            return

        # Calculate processing time
        elapsed_time = time.time() - start_time

        # Log processing summary
        logger.info(
            "Saved processed users: file=%s, lines=%d, inserted=%d, elapsed=%.3fs",
            str(file_path), total_lines, inserted_count, elapsed_time
        )

        # Upload to SFTP after successful database save
        try:
            remote_dir = f"{settings.SFTP_REMOTE_BASE}/processed_users"
            upload_file(str(file_path), remote_dir)
            logger.info("Processed file uploaded to SFTP: local=%s, remote_dir=%s", str(file_path), remote_dir)
        except Exception as e:
            logger.error("Failed to upload processed file to SFTP: local=%s, error=%s", str(file_path), str(e))
            # Continue without aborting - SFTP failure does not invalidate DB persistence

    async def handle_dlq_created(self, event: RedisEvent) -> None:
        """
        Process a dlq_created event by saving DLQ records to database.

        Args:
            event: Validated Redis event containing file path
        """
        file_path = Path(event.path)
        start_time = time.time()

        logger.info("Processing DLQ file: path=%s", str(file_path))

        # Check if file exists and is readable
        if not file_path.exists():
            logger.warning("DLQ file not found: path=%s", str(file_path))
            return

        if not file_path.is_file():
            logger.warning("DLQ path is not a file: path=%s", str(file_path))
            return

        # Process file line by line and save to database
        total_lines = 0
        inserted_count = 0
        current_ts = datetime.now(timezone.utc).isoformat()

        try:
            with get_conn() as conn:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue

                        total_lines += 1

                        try:
                            # Parse JSON with orjson fallback to json
                            try:
                                record = orjson.loads(line)
                            except orjson.JSONDecodeError:
                                record = json.loads(line)

                            # Extract fields for database
                            user_id = record.get("id")
                            error_reason = record.get("error_reason", "unknown")

                            # Insert into dlq_users table
                            conn.execute(
                                "INSERT INTO dlq_users (id, inserted_at, error_reason, data) VALUES (?, ?, ?, ?)",
                                (user_id, current_ts, error_reason, line)
                            )
                            inserted_count += 1

                        except (orjson.JSONDecodeError, json.JSONDecodeError) as e:
                            logger.debug("JSON parse error at line=%d: %s", line_num, str(e))
                        except Exception as e:
                            logger.debug("Database insert error at line=%d: %s", line_num, str(e))

                conn.commit()

        except Exception as e:
            logger.warning("Failed to process DLQ file: path=%s, error=%s", str(file_path), str(e))
            return

        # Calculate processing time
        elapsed_time = time.time() - start_time

        # Log processing summary
        logger.info(
            "Saved DLQ users: file=%s, lines=%d, inserted=%d, elapsed=%.3fs",
            str(file_path), total_lines, inserted_count, elapsed_time
        )

        # Upload to SFTP after successful database save
        try:
            remote_dir = f"{settings.SFTP_REMOTE_BASE}/dlq"
            upload_file(str(file_path), remote_dir)
            logger.info("DLQ file uploaded to SFTP: local=%s, remote_dir=%s", str(file_path), remote_dir)
        except Exception as e:
            logger.error("Failed to upload DLQ file to SFTP: local=%s, error=%s", str(file_path), str(e))
            # Continue without aborting - SFTP failure does not invalidate DB persistence

    def setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown on SIGINT/SIGTERM."""

        def signal_handler(signum: int, frame: object) -> None:
            logger.info("Received signal %d, initiating graceful shutdown", signum)
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def start(self) -> None:
        """
        Start consumer and process messages until shutdown signal.

        Initializes database schema, connects to Redis, subscribes to processed
        and DLQ channels, and processes messages continuously until graceful
        shutdown is requested.
        """
        self.setup_signal_handlers()

        logger.info("Starting saver consumer")

        # Initialize database schema
        init_schema()

        # Initialize Redis subscriber for all three channels
        channels = [settings.REDIS_CHANNEL_RAW, settings.REDIS_CHANNEL_PROCESSED, settings.REDIS_CHANNEL_DLQ]
        self.subscriber = RedisSubscriber(channels=channels)

        try:
            # Connect and subscribe
            await self.subscriber.connect()
            logger.info("Connected to Redis and subscribed to channels: %s", channels)

            # Start subscription loop in background
            subscription_task = asyncio.create_task(
                self.subscriber.subscribe(self.handle_message)
            )

            logger.info("Consumer started, waiting for messages...")

            # Wait for shutdown signal or subscription completion
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(self.shutdown_event.wait()),
                    subscription_task
                ],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            logger.info("Consumer shutdown complete (processed_events=%d)", self._processed_count)

        except Exception as e:
            logger.error("Consumer failed: %s", str(e), exc_info=True)
            raise

        finally:
            # Graceful cleanup
            if self.subscriber:
                self.subscriber.stop()
                await self.subscriber.close()
                logger.info("Redis subscriber connection closed")


async def main() -> None:
    """Main entry point for saver consumer."""
    run_once = os.getenv("RUN_ONCE", "false").lower() in ("true", "1", "yes")

    consumer = SaverConsumer(run_once=run_once)

    try:
        await consumer.start()
    except Exception as e:
        logger.error("Consumer failed: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())