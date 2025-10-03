"""
Extractor Module Entry Point

Allows execution via: python -m apps.extractor

Delegates to scheduler for all execution modes (scheduled and RUN_ONCE).
"""

import asyncio

from apps.extractor.scheduler import main

if __name__ == "__main__":
    asyncio.run(main())
