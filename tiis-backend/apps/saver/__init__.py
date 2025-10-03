"""
Saver App - Phase 3: Database Persistence

Responsibilities:
- Subscribe to Redis Pub/Sub channels: files.processed_users, files.dlq
- Persist file contents to SQLite database with separate tables
- Add inserted_at timestamps for all records
- Handle both processed user records and DLQ records

Outputs:
- SQLite tables: processed_users, dlq_users (with inserted_at timestamps)

Database Schema:
- processed_users(id INTEGER, inserted_at TEXT, data TEXT, department_code TEXT)
- dlq_users(id INTEGER, inserted_at TEXT, error_reason TEXT, data TEXT)
"""