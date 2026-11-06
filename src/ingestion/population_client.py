from __future__ import annotations

import math
from typing import Any


REQUIRED_COLUMNS = {"Nation ID", "Nation", "Year", "Population"}


def validate_population_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Data USA response must be a JSON object.")

    columns = payload.get("columns")
    records = payload.get("data")
    if not isinstance(columns, list) or not REQUIRED_COLUMNS.issubset(columns):
        raise ValueError(
            "Data USA response is missing required columns: "
            + ", ".join(sorted(REQUIRED_COLUMNS))
        )
    if not isinstance(records, list) or not records:
        raise ValueError("Data USA response must contain a non-empty 'data' list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"Data USA record {index} must be a JSON object.")
        if not REQUIRED_COLUMNS.issubset(record):
            raise ValueError(f"Data USA record {index} is missing required fields.")
        if not isinstance(record["Year"], int) or isinstance(record["Year"], bool):
            raise ValueError(f"Data USA record {index} has a non-integer Year.")
        if isinstance(record["Population"], bool) or not isinstance(
            record["Population"], (int, float)
        ):
            raise ValueError(f"Data USA record {index} has a non-numeric Population.")
        if not math.isfinite(record["Population"]):
            raise ValueError(f"Data USA record {index} has a non-finite Population.")
        if not isinstance(record["Nation"], str) or not isinstance(
            record["Nation ID"], str
        ):
            raise ValueError(f"Data USA record {index} has invalid Nation fields.")

    return payload
