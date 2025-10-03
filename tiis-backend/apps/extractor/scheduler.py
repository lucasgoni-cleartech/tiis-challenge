"""
Extraction Scheduler - Cron and On-Demand Execution

Manages scheduled and manual extraction job execution using APScheduler.

Features:
- Cron-based scheduling (configurable via EXTRACT_SCHEDULE_CRON)
- RUN_ONCE mode for immediate execution
- Redis event publishing after successful extraction
- Graceful shutdown handling

Usage:
    # Scheduled mode (default)
    python -m apps.extractor.scheduler

    # Run once and exit
    RUN_ONCE=true python -m apps.extractor.scheduler
"""

import asyncio
import logging
import os
import signal
import sys
from typing import NoReturn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apps.extractor.extractor_job import run_extraction
from apps.extractor.publisher import publish_extraction_event
from utils.config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class ExtractionScheduler:
    """
    Scheduler for periodic or on-demand extraction jobs.

    Handles:
    - APScheduler setup and management
    - Cron-based scheduling
    - RUN_ONCE immediate execution
    - Signal handling for graceful shutdown
    """

    def __init__(self, run_once: bool = False) -> None:
        """
        Initialize scheduler.

        Args:
            run_once: If True, run extraction once and exit
        """
        self.run_once = run_once
        self.scheduler: AsyncIOScheduler | None = None
        self.shutdown_event = asyncio.Event()

        logger.info(
            "ExtractionScheduler initialized",
            extra={
                "run_once": run_once,
                "cron_schedule": settings.EXTRACT_SCHEDULE_CRON,
            },
        )

    async def execute_extraction(self) -> None:
        """
        Execute extraction job and publish event.

        Handles errors and ensures event publishing on success.
        """
        logger.info("Starting extraction execution")

        try:
            # Run extraction
            output_file = await run_extraction()

            # Publish event
            await publish_extraction_event(output_file)

            logger.info(
                "Extraction execution completed successfully",
                extra={"output_file": output_file},
            )

        except Exception as e:
            logger.error(
                "Extraction execution failed",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

        finally:
            # Signal shutdown if run_once mode
            if self.run_once:
                logger.info("RUN_ONCE mode: signaling shutdown")
                self.shutdown_event.set()

    def setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown on SIGINT/SIGTERM."""

        def signal_handler(signum: int, frame: object) -> None:
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def start(self) -> None:
        """
        Start scheduler or execute once.

        In scheduled mode, runs continuously until shutdown signal.
        In RUN_ONCE mode, executes immediately and exits.
        """
        self.setup_signal_handlers()

        if self.run_once:
            logger.info("Running in RUN_ONCE mode")
            await self.execute_extraction()
            return

        # Scheduled mode
        logger.info("Running in scheduled mode")

        self.scheduler = AsyncIOScheduler()

        # Add cron job
        trigger = CronTrigger.from_crontab(settings.EXTRACT_SCHEDULE_CRON)
        self.scheduler.add_job(
            self.execute_extraction,
            trigger=trigger,
            id="extraction_job",
            name="Periodic User Extraction",
            replace_existing=True,
        )

        # Start scheduler first to get next_run_time
        self.scheduler.start()
        logger.info("Scheduler started")

        # Get next run time after scheduler has started
        job = self.scheduler.get_job("extraction_job")
        next_run = getattr(job, "next_run_time", None)
        next_run_str = str(next_run) if next_run is not None else None

        logger.info(
            "Scheduled extraction job",
            extra={
                "schedule": settings.EXTRACT_SCHEDULE_CRON,
                "next_run": next_run_str,
            },
        )
        logger.info("Waiting for jobs...")

        # Wait for shutdown signal
        await self.shutdown_event.wait()

        # Graceful shutdown
        logger.info("Shutting down scheduler")
        if self.scheduler:
            self.scheduler.shutdown(wait=True)
        logger.info("Scheduler shutdown complete")


async def main() -> None:
    """Main entry point for scheduler."""
    run_once = os.getenv("RUN_ONCE", "false").lower() in ("true", "1", "yes")

    scheduler = ExtractionScheduler(run_once=run_once)

    try:
        await scheduler.start()
    except Exception as e:
        logger.error("Scheduler failed", extra={"error": str(e)}, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
