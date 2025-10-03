"""
Transformer App - Phase 2: Transformation and Validation

Responsibilities:
- Subscribe to Redis Pub/Sub channel: files.raw_users
- Validate records against Pydantic schema (id, firstName, email, age, company.department)
- Enrich data with departments.csv lookup (department → department_code)
- Separate valid vs invalid records (DLQ pattern)
- Publish transformation results to Redis Pub/Sub

Outputs:
- processed_users/etl_[YYYYMMDD_HHMMSS].jsonl (valid records)
- dlq/invalid_users_[YYYYMMDD_HHMMSS].jsonl (invalid records with error_reason)
- Redis events:
  - channel=files.processed_users, payload={type, path, ts}
  - channel=files.dlq, payload={type, path, ts}

TODO:
- Implement Redis subscriber for files.raw_users
- Create Pydantic validation schemas
- Load and apply departments.csv enrichment
- Implement DLQ logic with error_reason
- Integrate Redis publisher for output events
"""
