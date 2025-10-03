"""
CSV Lookup Utilities

Provides functions to load and cache lookup tables from CSV files.
"""

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_departments(path: str) -> dict[str, str]:
    """
    Load departments lookup table from CSV file.

    Reads CSV with header 'department,department_code' and builds a mapping
    from department name (lowercase, stripped) to department code.

    Args:
        path: Path to departments CSV file

    Returns:
        Dictionary mapping department.lower().strip() → department_code

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        IOError: If CSV file can't be read
        ValueError: If CSV format is invalid
    """
    csv_path = Path(path)

    if not csv_path.exists():
        error_msg = f"Departments CSV not found: {path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    if not csv_path.is_file():
        error_msg = f"Departments path is not a file: {path}"
        logger.error(error_msg)
        raise IOError(error_msg)

    dept_map = {}

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Validate expected headers
            if 'department' not in reader.fieldnames or 'department_code' not in reader.fieldnames:
                error_msg = f"Invalid CSV format: expected 'department,department_code' headers in {path}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            for row_num, row in enumerate(reader, 2):  # Start at 2 for header line
                department = row.get('department', '').strip()
                department_code = row.get('department_code', '').strip()

                if not department or not department_code:
                    logger.warning(
                        "Skipping invalid row in departments CSV",
                        extra={
                            "file_path": path,
                            "row_number": row_num,
                            "department": department,
                            "department_code": department_code
                        }
                    )
                    continue

                # Map department name (lowercase) to department code (preserve case)
                dept_key = department.lower().strip()
                dept_map[dept_key] = department_code

    except IOError as e:
        error_msg = f"Failed to read departments CSV: {path} - {str(e)}"
        logger.error(error_msg)
        raise IOError(error_msg) from e

    except csv.Error as e:
        error_msg = f"Invalid CSV format in departments file: {path} - {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e

    if not dept_map:
        error_msg = f"No valid departments found in CSV: {path}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return dept_map