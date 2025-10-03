"""
Extractor App - Phase 1: The Resilient Extractor

Responsibilities:
- Scheduled execution (daily cron via APScheduler)
- Resumable pagination from DummyJSON API (limit/skip with state persistence)
- Exponential backoff retry strategy (3 attempts)
- Output raw user data to JSONL files
- Publish Redis Pub/Sub events on successful extraction

Output:
- raw_users/records_[YYYYMMDD_HHMMSS].jsonl
- Redis event: channel=files.raw_users, payload={type, path, ts}

TODO:
- Implement main extractor logic
- Add APScheduler configuration
- Implement state management (last_skip.json)
- Add tenacity retry decorator
- Integrate Redis publisher
"""
