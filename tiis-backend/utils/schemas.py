"""
Pydantic Schemas - Data Validation Models

Defines all Pydantic schemas for data validation throughout the pipeline:
- API responses
- User records
- Redis Pub/Sub messages
- Database models

Usage:
    from utils.schemas import UserRecord

    user = UserRecord(**raw_data)
    if user.age < 18 or user.age > 65:
        raise ValidationError("Age out of range")

TODO:
- Implement UserRecord schema (id, firstName, email, age, company.department)
- Add DepartmentEnrichment schema
- Create RedisEvent schema for Pub/Sub messages
- Add validation rules (email format, age range 18-65)
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, EmailStr, Field, validator


class UserRecord(BaseModel):
    """User record schema with validation rules.

    Validates against requirements:
    - id: integer
    - firstName: string, non-empty
    - email: valid email format
    - age: number, between 18 and 65 inclusive
    - company.department: string, non-empty

    TODO: Complete implementation
    """

    id: int = Field(..., description="User ID")
    firstName: str = Field(..., min_length=1, description="First name")
    email: EmailStr = Field(..., description="Email address")
    age: int = Field(..., ge=18, le=65, description="Age (18-65)")
    company: dict[str, Any] = Field(..., description="Company information")

    @validator("company")
    def validate_department(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Validate company.department field exists and is non-empty."""
        if not isinstance(v, dict):
            raise ValueError("company must be a dictionary")

        department = v.get("department")
        if not department or not isinstance(department, str) or not department.strip():
            raise ValueError("company.department must be a non-empty string")

        return v


class DepartmentEnrichment(BaseModel):
    """Department enrichment data from departments.csv.

    TODO: Define schema based on departments.csv structure
    """

    pass


class RedisEvent(BaseModel):
    """Redis Pub/Sub event payload.

    Standard format for file creation events:
    {
        "type": "raw_created" | "processed_created" | "dlq_created",
        "path": "/data/raw_users/records_20250115_031500.jsonl",
        "ts": "2025-01-15T03:15:02Z"
    }

    TODO: Implement event schema
    """

    type: str = Field(..., description="Event type")
    path: str = Field(..., description="File path")
    ts: datetime = Field(default_factory=datetime.utcnow, description="Timestamp")


class DLQRecord(BaseModel):
    """Dead-letter queue record with error information.

    Contains original record plus error_reason field.

    TODO: Implement DLQ schema
    """

    pass
