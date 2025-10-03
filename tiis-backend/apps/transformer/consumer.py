"""
Transformer Consumer - Redis Pub/Sub Event Handler

Consumes raw file creation events from Redis Pub/Sub and processes them for validation
and transformation. This is the entry point for Phase 2 of the ETL pipeline.

Features:
- Redis Pub/Sub subscription via production wrapper
- Event validation and filtering
- Graceful shutdown handling
- Structured logging

Usage:
    # Consumer mode (default)
    python -m apps.transformer.consumer

    # For development/testing
    RUN_ONCE=true python -m apps.transformer.consumer
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
from utils.lookup import load_departments
from utils.mq import RedisPublisher, RedisSubscriber
from utils.schemas import RedisEvent, UserRecord

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class TransformerConsumer:
    """
    Consumer for processing raw file creation events from Redis Pub/Sub.

    Handles:
    - Redis subscription management
    - Event validation and filtering
    - Signal handling for graceful shutdown
    """

    def __init__(self, run_once: bool = False) -> None:
        """
        Initialize transformer consumer.

        Args:
            run_once: If True, process one event and exit (for testing)
        """
        self.run_once = run_once
        self.subscriber: RedisSubscriber | None = None
        self.shutdown_event = asyncio.Event()
        self._processed_count = 0
        self._dept_map: dict[str, str] = {}

        logger.info(
            "TransformerConsumer initialized",
            extra={
                "run_once": run_once,
                "target_channel": settings.REDIS_CHANNEL_RAW,
            },
        )

    async def handle_message(self, channel: str, message: Dict[str, Any]) -> None:
        """
        Handle incoming Redis Pub/Sub message.

        Validates event structure and processes raw_created events.

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
                logger.warning(
                    "Invalid event payload",
                    extra={"channel": channel, "message": message, "error": str(e)}
                )
                return

            # Only handle raw_created events
            if event.type != "raw_created":
                logger.debug(
                    "Ignoring non-raw_created event",
                    extra={"channel": channel, "event_type": event.type}
                )
                return

            # Process the raw file
            await self.handle_raw_created(event)

            self._processed_count += 1

            # Signal shutdown if run_once mode
            if self.run_once:
                logger.info("RUN_ONCE mode: signaling shutdown after processing event")
                self.shutdown_event.set()

        except Exception as e:
            logger.error(
                "Failed to process message",
                extra={
                    "channel": channel,
                    "message": message,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

    async def handle_raw_created(self, event: RedisEvent) -> None:
        """
        Process a raw_created event by validating, enriching, and writing user records.

        Args:
            event: Validated Redis event containing file path

        Raises:
            Exception: If file processing fails critically
        """
        file_path = Path(event.path)
        start_time = time.time()

        logger.info("Processing raw file: path=%s, ts=%s", str(file_path), getattr(event, 'ts', None))

        # Check if file exists and is readable
        if not file_path.exists():
            logger.warning("File not found: path=%s", str(file_path))
            return

        if not file_path.is_file():
            logger.warning("Path is not a file: path=%s", str(file_path))
            return

        # Create timestamped output paths
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        processed_path = f"{settings.PROCESSED_DIR}/etl_{timestamp}.jsonl"
        dlq_path = f"{settings.DLQ_DIR}/invalid_users_{timestamp}.jsonl"

        # Ensure parent directories exist
        Path(processed_path).parent.mkdir(parents=True, exist_ok=True)
        Path(dlq_path).parent.mkdir(parents=True, exist_ok=True)

        # Process file line by line
        total_lines = 0
        processed_count = 0
        dlq_count = 0
        first_valid_record_keys = None

        processed_file = None
        dlq_file = None
        processed_file_opened = False
        dlq_file_opened = False

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    total_lines += 1

                    try:
                        # Parse JSON with orjson fallback to json
                        try:
                            raw_dict = orjson.loads(line)
                        except orjson.JSONDecodeError:
                            raw_dict = json.loads(line)

                        # Validate with UserRecord schema
                        user_record = UserRecord(**raw_dict)

                        # Enrichment: lookup department code
                        dept = raw_dict["company"]["department"]
                        dept_code = self._dept_map.get(dept.lower().strip())

                        if dept_code is None:
                            # Unknown department - send to DLQ
                            dlq_record = {
                                **raw_dict,
                                "error_reason": "unknown_department"
                            }
                            if dlq_file is None:
                                dlq_file = open(dlq_path, 'w', encoding='utf-8')
                                if not dlq_file_opened:
                                    logger.info("Opening DLQ output: %s", dlq_path)
                                    dlq_file_opened = True
                            dlq_file.write(orjson.dumps(dlq_record).decode('utf-8') + '\n')
                            dlq_count += 1

                            logger.debug("Unknown department at line=%d: %s", line_num, dept)
                        else:
                            # Add department code and mark as processed
                            enriched_dict = {**raw_dict}
                            enriched_dict["company"]["department_code"] = dept_code

                            if processed_file is None:
                                processed_file = open(processed_path, 'w', encoding='utf-8')
                                if not processed_file_opened:
                                    logger.info("Opening processed output: %s", processed_path)
                                    processed_file_opened = True
                            processed_file.write(orjson.dumps(enriched_dict).decode('utf-8') + '\n')
                            processed_count += 1

                            # Capture first valid record keys for logging
                            if first_valid_record_keys is None:
                                first_valid_record_keys = list(raw_dict.keys())

                    except (orjson.JSONDecodeError, json.JSONDecodeError) as e:
                        # JSON parse error - send to DLQ
                        dlq_record = {
                            "raw_line": line,
                            "error_reason": "invalid_json"
                        }
                        if dlq_file is None:
                            dlq_file = open(dlq_path, 'w', encoding='utf-8')
                            if not dlq_file_opened:
                                logger.info("Opening DLQ output: %s", dlq_path)
                                dlq_file_opened = True
                        dlq_file.write(orjson.dumps(dlq_record).decode('utf-8') + '\n')
                        dlq_count += 1

                        logger.debug("JSON parse error at line=%d: %s", line_num, str(e))
                    except ValidationError as e:
                        # Validation error - send to DLQ
                        dlq_record = {
                            **raw_dict,
                            "error_reason": str(e).split('\n')[0]  # First line of validation error
                        }
                        if dlq_file is None:
                            dlq_file = open(dlq_path, 'w', encoding='utf-8')
                            if not dlq_file_opened:
                                logger.info("Opening DLQ output: %s", dlq_path)
                                dlq_file_opened = True
                        dlq_file.write(orjson.dumps(dlq_record).decode('utf-8') + '\n')
                        dlq_count += 1

                        logger.debug("Validation error at line=%d: %s", line_num, str(e))

        except IOError as e:
            logger.warning(
                "Failed to read file",
                extra={
                    "file_path": str(file_path),
                    "error": str(e)
                }
            )
            return

        finally:
            # Close output files
            if processed_file:
                processed_file.close()
                if processed_count > 0:
                    logger.info("Processed file written: %s", processed_path)
            if dlq_file:
                dlq_file.close()
                if dlq_count > 0:
                    logger.info("DLQ file updated: %s", dlq_path)

        # Calculate processing time
        elapsed_time = time.time() - start_time

        # Log processing summary
        logger.info(
            "File processing complete: path=%s, total_lines=%d, valid=%d, invalid=%d, elapsed=%.3fs",
            str(file_path), total_lines, processed_count, dlq_count, elapsed_time
        )

        # Publish Redis events
        await self._publish_output_events(processed_path, dlq_path, processed_count, dlq_count)

    async def _publish_output_events(self, processed_path: str, dlq_path: str, processed_count: int, dlq_count: int) -> None:
        """
        Publish Redis events for processed and DLQ files.

        Args:
            processed_path: Path to processed file
            dlq_path: Path to DLQ file
            processed_count: Number of processed records
            dlq_count: Number of DLQ records
        """
        publisher = RedisPublisher()
        current_ts = datetime.now(timezone.utc).isoformat()

        try:
            # Publish processed event if we have processed records
            if processed_count > 0:
                processed_message = {
                    "type": "processed_created",
                    "path": processed_path,
                    "ts": current_ts,
                    "channel": settings.REDIS_CHANNEL_PROCESSED,
                }
                await publisher.publish(settings.REDIS_CHANNEL_PROCESSED, processed_message)

                logger.info("Published processed event to %s: %s", settings.REDIS_CHANNEL_PROCESSED, processed_path)

            # Publish DLQ event if we have DLQ records
            if dlq_count > 0:
                dlq_message = {
                    "type": "dlq_created",
                    "path": dlq_path,
                    "ts": current_ts,
                    "channel": settings.REDIS_CHANNEL_DLQ,
                }
                await publisher.publish(settings.REDIS_CHANNEL_DLQ, dlq_message)

                logger.info("Published DLQ event to %s: %s", settings.REDIS_CHANNEL_DLQ, dlq_path)

        except Exception as e:
            logger.error(
                "Failed to publish output events",
                extra={
                    "processed_path": processed_path,
                    "dlq_path": dlq_path,
                    "error": str(e)
                },
                exc_info=True
            )
            raise

        finally:
            await publisher.close()

    def setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown on SIGINT/SIGTERM."""

        def signal_handler(signum: int, frame: object) -> None:
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def start(self) -> None:
        """
        Start consumer and process messages until shutdown signal.

        Connects to Redis, subscribes to raw_users channel, and processes messages
        continuously until graceful shutdown is requested.
        """
        self.setup_signal_handlers()

        logger.info("Starting transformer consumer")

        # Ensure departments CSV exists (copy from image resource if missing)
        try:
            from pathlib import Path
            import os, shutil
            csv_target = Path(getattr(settings, "DEPARTMENTS_CSV", "/data/departments.csv"))
            if not csv_target.exists():
                src = Path("/app/resources/departments.csv")
                if src.exists():
                    csv_target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, csv_target)
                    logger.info("Seeded departments CSV at %s from %s", str(csv_target), str(src))
        except Exception as _e:
            logger.warning("Could not ensure departments CSV: %s", _e)

        # Load departments lookup table
        self._dept_map = load_departments(settings.DEPARTMENTS_CSV)
        logger.info(
            "Departments lookup loaded (count=%s, csv=%s)",
            len(self._dept_map),
            settings.DEPARTMENTS_CSV,
        )

        # Initialize Redis subscriber
        self.subscriber = RedisSubscriber(channels=[settings.REDIS_CHANNEL_RAW])

        try:
            # Connect and subscribe
            await self.subscriber.connect()
            logger.info(
                "Connected to Redis and subscribed to channel",
                extra={"channel": settings.REDIS_CHANNEL_RAW}
            )

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

            logger.info(
                "Consumer shutdown complete",
                extra={"processed_events": self._processed_count}
            )

        except Exception as e:
            logger.error("Consumer failed", extra={"error": str(e)}, exc_info=True)
            raise

        finally:
            # Graceful cleanup
            if self.subscriber:
                self.subscriber.stop()
                await self.subscriber.close()
                logger.info("Redis subscriber connection closed")


async def main() -> None:
    """Main entry point for transformer consumer."""
    run_once = os.getenv("RUN_ONCE", "false").lower() in ("true", "1", "yes")

    consumer = TransformerConsumer(run_once=run_once)

    try:
        await consumer.start()
    except Exception as e:
        logger.error("Consumer failed", extra={"error": str(e)}, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())