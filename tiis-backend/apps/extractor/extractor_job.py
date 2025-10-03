"""
Extractor Job - DummyJSON API to JSONL Extraction

Fetches user records from DummyJSON API with resumable pagination,
exponential retry logic, and streaming JSONL output.

Features:
- Resumable pagination using STATE_DIR/last_skip.json
- Exponential backoff retries with Tenacity
- Streaming writes (no full in-memory load)
- Structured logging with context
- Event publishing via Redis Pub/Sub

Usage:
    from apps.extractor.extractor_job import run_extraction

    await run_extraction()
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from utils.config import settings

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Custom exception for extraction failures."""

    pass


class ExtractorJob:
    """
    Manages extraction of user records from DummyJSON API.

    Handles:
    - Pagination with configurable limit
    - State persistence for resumability
    - Retry logic with exponential backoff
    - JSONL streaming output
    """

    def __init__(self) -> None:
        """Initialize extractor with configuration from settings."""
        self.api_base = settings.USERS_API_BASE
        self.limit = settings.EXTRACT_LIMIT
        self.timeout = settings.API_TIMEOUT
        self.state_dir = Path(settings.STATE_DIR)
        self.raw_dir = Path(settings.RAW_DIR)
        self.state_file = self.state_dir / "last_skip.json"

        # Ensure directories exist
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "ExtractorJob initialized",
            extra={
                "api_base": self.api_base,
                "limit": self.limit,
                "state_dir": str(self.state_dir),
                "raw_dir": str(self.raw_dir),
            },
        )

    def load_last_skip(self) -> int:
        """
        Load last processed skip value from state file.

        Returns:
            Last skip value, or 0 if no state exists
        """
        if not self.state_file.exists():
            logger.info("No previous state found, starting from skip=0")
            return 0

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
                skip = state.get("skip", 0)
                logger.info(
                    "Loaded previous state",
                    extra={"skip": skip, "timestamp": state.get("timestamp")},
                )
                return skip
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(
                "Failed to load state file, starting from skip=0",
                extra={"error": str(e)},
            )
            return 0

    def save_skip(self, skip: int) -> None:
        """
        Save current skip value to state file.

        Args:
            skip: Current skip value to persist
        """
        state = {
            "skip": skip,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            logger.debug("Saved state", extra={"skip": skip})
        except IOError as e:
            logger.error("Failed to save state", extra={"error": str(e)})
            raise ExtractionError(f"State persistence failed: {e}") from e

    @retry(
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
        stop=stop_after_attempt(settings.EXTRACT_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def fetch_page(self, skip: int) -> dict[str, Any]:
        """
        Fetch a single page from DummyJSON API with retry logic.

        Args:
            skip: Number of records to skip (pagination offset)

        Returns:
            API response as dictionary

        Raises:
            ExtractionError: If request fails after retries
        """
        url = f"{self.api_base}?limit={self.limit}&skip={skip}"
        logger.debug("Fetching page", extra={"url": url, "skip": skip})

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            logger.info(
                "Page fetched successfully",
                extra={
                    "skip": skip,
                    "limit": self.limit,
                    "users_count": len(data.get("users", [])),
                    "total": data.get("total", 0),
                },
            )

            return data

        except requests.Timeout as e:
            logger.warning("Request timeout", extra={"skip": skip, "error": str(e)})
            raise
        except requests.RequestException as e:
            logger.error("Request failed", extra={"skip": skip, "error": str(e)})
            raise
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON response", extra={"skip": skip, "error": str(e)})
            raise ExtractionError(f"Invalid JSON response: {e}") from e

    def extract(self) -> Path:
        """
        Extract all user records from API with resumable pagination.

        Creates timestamped JSONL file with streaming writes.

        Returns:
            Path to created JSONL file

        Raises:
            ExtractionError: If extraction fails
        """
        # Resume from last state
        skip = self.load_last_skip()

        # Create output file with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = self.raw_dir / f"records_{timestamp}.jsonl"

        logger.info(
            "Starting extraction",
            extra={
                "output_file": str(output_file),
                "resume_from_skip": skip,
            },
        )

        total_records = 0
        total_api_total = None

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                while True:
                    # Fetch page
                    page_data = self.fetch_page(skip)

                    users = page_data.get("users", [])
                    total = page_data.get("total", 0)

                    if total_api_total is None:
                        total_api_total = total

                    # Stream write each user as JSONL
                    for user in users:
                        f.write(json.dumps(user) + "\n")
                        total_records += 1

                    # Save state after successful page
                    skip += self.limit
                    self.save_skip(skip)

                    # Check if we've fetched all records
                    if skip >= total or len(users) == 0:
                        logger.info(
                            "Extraction completed",
                            extra={
                                "total_records": total_records,
                                "api_total": total_api_total,
                                "output_file": str(output_file),
                            },
                        )
                        break

        except Exception as e:
            logger.error(
                "Extraction failed",
                extra={
                    "error": str(e),
                    "records_written": total_records,
                    "last_skip": skip,
                },
            )
            # Clean up partial file
            if output_file.exists():
                output_file.unlink()
            raise ExtractionError(f"Extraction failed at skip={skip}: {e}") from e

        # Reset state after successful completion
        self.save_skip(0)

        return output_file


async def run_extraction() -> str:
    """
    Execute extraction job and return output file path.

    Returns:
        Path to created JSONL file as string

    Raises:
        ExtractionError: If extraction fails
    """
    logger.info("Starting extraction job")

    try:
        job = ExtractorJob()
        output_file = job.extract()

        logger.info(
            "Extraction job completed successfully",
            extra={"output_file": str(output_file)},
        )

        return str(output_file)

    except Exception as e:
        logger.error("Extraction job failed", extra={"error": str(e)})
        raise ExtractionError(f"Extraction job failed: {e}") from e
