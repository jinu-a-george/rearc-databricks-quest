from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.bls_client import discover_file_urls
from src.ingestion.config import BLS_DIRECTORY_URL, POPULATION_API_URL
from src.ingestion.manifest import utc_timestamp
from src.ingestion.population_client import validate_population_payload


LOGGER = logging.getLogger(__name__)
DELIMITERS = ("\t", ",", ";", "|")
MISSING_TOKENS = {"na", "n/a", "null", "none", "nan", "(null)", "missing"}
TYPE_PATTERNS = {
    "integer": re.compile(r"^[+-]?\d+$"),
    "decimal": re.compile(
        r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
    ),
}
TYPE_SAMPLE_LIMIT = 5


def _is_missing_token(value: str) -> bool:
    return value.strip().casefold() in MISSING_TOKENS


def _matches_type(value: Any, type_name: str) -> bool:
    if type_name == "boolean":
        return isinstance(value, bool) or (
            isinstance(value, str)
            and value.strip().casefold() in {"true", "false", "yes", "no"}
        )
    if type_name == "integer":
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        return isinstance(value, str) and bool(
            TYPE_PATTERNS["integer"].fullmatch(value.strip())
        )
    if type_name == "decimal":
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return math.isfinite(value)
        if not isinstance(value, str) or not TYPE_PATTERNS["decimal"].fullmatch(
            value.strip()
        ):
            return False
        try:
            return math.isfinite(float(value.strip()))
        except ValueError:
            return False
    if type_name == "date":
        if isinstance(value, datetime):
            return True
        if not isinstance(value, str):
            return False
        try:
            datetime.fromisoformat(value.strip())
            return True
        except ValueError:
            return False
    return False


def _profile_column(
    values: Iterable[Any], row_count: int
) -> dict[str, Any]:
    null_token_counts: Counter[str] = Counter()
    candidate_counts: Counter[str] = Counter()
    non_missing_count = 0
    blank_count = 0
    suspicious_char_count = 0
    type_mismatch_samples: dict[str, list[str]] = {}

    for raw_value in values:
        if raw_value is None:
            null_token_counts["JSON null"] += 1
            continue

        value = raw_value.strip() if isinstance(raw_value, str) else raw_value
        if isinstance(value, str) and not value:
            blank_count += 1
            continue
        if isinstance(value, str) and _is_missing_token(value):
            null_token_counts[value.casefold()] += 1
            continue

        non_missing_count += 1
        if isinstance(value, str) and (
            "\ufffd" in value or any(ord(char) < 32 for char in value)
        ):
            suspicious_char_count += 1

        for type_name in ("integer", "decimal", "boolean", "date"):
            if _matches_type(value, type_name):
                candidate_counts[type_name] += 1
            else:
                samples = type_mismatch_samples.setdefault(type_name, [])
                if len(samples) < TYPE_SAMPLE_LIMIT and str(value) not in samples:
                    samples.append(str(value)[:100])

    candidates = [
        {
            "type": type_name,
            "matched_values": candidate_counts[type_name],
            "coverage": (
                round(candidate_counts[type_name] / non_missing_count, 4)
                if non_missing_count
                else 0.0
            ),
        }
        for type_name in ("integer", "decimal", "boolean", "date")
    ]
    full_matches = [
        item["type"]
        for item in candidates
        if non_missing_count and item["matched_values"] == non_missing_count
    ]
    inferred_type = full_matches[0] if full_matches else "string_or_mixed"

    best_candidate = max(candidates, key=lambda item: item["coverage"])
    nonconforming_count = 0
    nonconforming_samples: list[str] = []
    if best_candidate["coverage"] >= 0.5 and best_candidate["coverage"] < 1:
        nonconforming_count = non_missing_count - best_candidate["matched_values"]
        nonconforming_samples = type_mismatch_samples[best_candidate["type"]]

    return {
        "row_count": row_count,
        "non_missing_count": non_missing_count,
        "blank_count": blank_count,
        "missing_token_counts": dict(sorted(null_token_counts.items())),
        "potential_types": candidates,
        "inferred_type": inferred_type,
        "suspected_type_mismatch_count": nonconforming_count,
        "suspected_type_mismatch_samples": nonconforming_samples,
        "suspicious_character_count": suspicious_char_count,
    }


def _detect_delimiter(path: Path) -> str | None:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as source:
        sample = source.read(64 * 1024)
    if not sample.strip():
        return None

    lines = [line for line in sample.splitlines() if line.strip()][:100]
    best: tuple[float, int, str] | None = None
    for priority, delimiter in enumerate(DELIMITERS):
        counts = [len(line.split(delimiter)) for line in lines]
        multi_column_counts = [count for count in counts if count > 1]
        if not multi_column_counts:
            continue
        common_count, common_frequency = Counter(multi_column_counts).most_common(1)[0]
        consistency = common_frequency / len(lines)
        candidate = (consistency, common_count, delimiter)
        if best is None or candidate[:2] > best[:2]:
            best = candidate
        elif candidate[:2] == best[:2] and priority < DELIMITERS.index(best[2]):
            best = candidate

    if best is None or best[0] < 0.6:
        return None
    return best[2]


def profile_delimited_file(path: Path) -> dict[str, Any]:
    delimiter = _detect_delimiter(path)
    if delimiter is None:
        with path.open("r", encoding="utf-8-sig", errors="replace") as source:
            line_count = sum(1 for _ in source)
        return {
            "file_type": "plain_text",
            "delimiter": None,
            "header": None,
            "columns": [],
            "row_count": line_count,
            "malformed_row_count": 0,
            "suspected_garbage": {
                "malformed_row_count": 0,
                "suspicious_character_count": 0,
            },
        }

    columns: list[str] = []
    column_values: list[list[str]] = []
    row_count = 0
    malformed_rows = 0
    malformed_samples: list[dict[str, Any]] = []
    suspicious_character_count = 0

    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as source:
        reader = csv.reader(source, delimiter=delimiter)
        header = next(reader, None)
        if header is None:
            return {
                "file_type": "delimited_text",
                "delimiter": _delimiter_name(delimiter),
                "header": [],
                "columns": [],
                "row_count": 0,
                "malformed_row_count": 0,
                "suspected_garbage": {
                    "malformed_row_count": 0,
                    "suspicious_character_count": 0,
                },
            }

        columns = [name.strip() for name in header]
        column_values = [[] for _ in columns]
        for row in reader:
            if not row or all(not value.strip() for value in row):
                continue
            row_count += 1
            if len(row) != len(columns):
                malformed_rows += 1
                if len(malformed_samples) < TYPE_SAMPLE_LIMIT:
                    malformed_samples.append(
                        {
                            "line_number": reader.line_num,
                            "expected_fields": len(columns),
                            "actual_fields": len(row),
                            "sample": row[:TYPE_SAMPLE_LIMIT],
                        }
                    )
            for index in range(min(len(row), len(column_values))):
                value = row[index]
                column_values[index].append(value)
                suspicious_character_count += int(
                    "\ufffd" in value
                    or any(ord(character) < 32 for character in value)
                )

    profiled_columns = []
    for index, name in enumerate(columns):
        profile = _profile_column(column_values[index], row_count)
        profiled_columns.append(
            {
                "index": index,
                "name": name,
                **profile,
            }
        )

    return {
        "file_type": "delimited_text",
        "delimiter": _delimiter_name(delimiter),
        "header": columns,
        "columns": profiled_columns,
        "row_count": row_count,
        "malformed_row_count": malformed_rows,
        "malformed_row_samples": malformed_samples,
        "suspected_garbage": {
            "malformed_row_count": malformed_rows,
            "suspicious_character_count": suspicious_character_count,
        },
    }


def _delimiter_name(delimiter: str) -> str:
    return {"\t": "tab", ",": "comma", ";": "semicolon", "|": "pipe"}[delimiter]


def profile_json_payload(payload: dict[str, Any]) -> dict[str, Any]:
    records = payload["data"]
    field_names = payload["columns"]
    columns = []
    for index, name in enumerate(field_names):
        values = [record.get(name) for record in records]
        columns.append(
            {
                "index": index,
                "name": name,
                **_profile_column(values, len(records)),
            }
        )

    return {
        "file_type": "json_records",
        "delimiter": "JSON (not delimited)",
        "header": field_names,
        "columns": columns,
        "row_count": len(records),
        "malformed_row_count": 0,
        "suspected_garbage": {
            "malformed_row_count": 0,
            "suspicious_character_count": sum(
                column["suspicious_character_count"] for column in columns
            ),
        },
    }


def _read_tsv_records(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"Expected a tab-separated header in {path}")
        return [
            {
                (name or "").strip(): (value or "").strip()
                for name, value in row.items()
                if name is not None
            }
            for row in reader
            if row
        ]


def _observation_key(record: dict[str, str]) -> tuple[str, str, str]:
    return (
        record.get("series_id", ""),
        record.get("year", ""),
        record.get("period", ""),
    )


def _duplicate_key_profile(
    records: list[dict[str, str]],
) -> dict[str, Any]:
    key_counts = Counter(_observation_key(record) for record in records)
    duplicates = [
        {"key": list(key), "count": count}
        for key, count in key_counts.items()
        if count > 1
    ]
    return {
        "row_count": len(records),
        "distinct_key_count": len(key_counts),
        "duplicate_key_count": len(duplicates),
        "extra_duplicate_rows": sum(item["count"] - 1 for item in duplicates),
        "duplicate_examples": duplicates[:TYPE_SAMPLE_LIMIT],
    }


def _lookup_values(
    bls_dir: Path, filename: str, code_field: str, value_field: str
) -> dict[str, str]:
    return {
        record.get(code_field, ""): record.get(value_field, "")
        for record in _read_tsv_records(bls_dir / filename)
        if record.get(code_field, "")
    }


def profile_additional_checks(
    bls_dir: Path, population_payload: dict[str, Any]
) -> dict[str, Any]:
    all_data = _read_tsv_records(bls_dir / "pr.data.1.AllData")
    current = _read_tsv_records(bls_dir / "pr.data.0.Current")
    series = _read_tsv_records(bls_dir / "pr.series")
    period_lookup = _lookup_values(
        bls_dir, "pr.period", "period", "period_name"
    )
    sector_lookup = _lookup_values(
        bls_dir, "pr.sector", "sector_code", "sector_name"
    )
    class_lookup = _lookup_values(
        bls_dir, "pr.class", "class_code", "class_text"
    )
    measure_lookup = _lookup_values(
        bls_dir, "pr.measure", "measure_code", "measure_text"
    )
    duration_lookup = _lookup_values(
        bls_dir, "pr.duration", "duration_code", "duration_text"
    )
    seasonal_lookup = _lookup_values(
        bls_dir, "pr.seasonal", "Seasonal_code", "Seasonal_text"
    )
    footnote_lookup = _lookup_values(
        bls_dir, "pr.footnote", "footnote_code", "footnote_text"
    )

    lookup_definitions = {
        "sector_code": sector_lookup,
        "class_code": class_lookup,
        "measure_code": measure_lookup,
        "duration_code": duration_lookup,
        "seasonal": seasonal_lookup,
    }
    series_lookup_integrity: dict[str, Any] = {}
    for field, lookup in lookup_definitions.items():
        fact_codes = {record.get(field, "") for record in series}
        missing_codes = sorted(code for code in fact_codes if code not in lookup)
        series_lookup_integrity[field] = {
            "distinct_series_codes": len(fact_codes),
            "missing_code_count": len(missing_codes),
            "missing_code_examples": missing_codes[:TYPE_SAMPLE_LIMIT],
        }

    series_by_id = {record.get("series_id", ""): record for record in series}
    all_data_series_ids = {record.get("series_id", "") for record in all_data}
    current_series_ids = {record.get("series_id", "") for record in current}
    missing_series_ids = sorted(all_data_series_ids - series_by_id.keys())
    current_missing_series_ids = sorted(current_series_ids - series_by_id.keys())
    target_series_id = "PRS30006032"
    target_records = [
        record for record in all_data
        if record.get("series_id") == target_series_id
    ]
    target_q01_records = [
        record for record in target_records if record.get("period") == "Q01"
    ]
    target_metadata = series_by_id.get(target_series_id)
    target_description = None
    if target_metadata:
        description_fields = (
            ("sector_code", sector_lookup),
            ("class_code", class_lookup),
            ("measure_code", measure_lookup),
            ("duration_code", duration_lookup),
            ("seasonal", seasonal_lookup),
        )
        description_parts = [
            lookup.get(target_metadata.get(field, ""), "")
            for field, lookup in description_fields
        ]
        if all(description_parts):
            target_description = " — ".join(description_parts)

    all_data_key_counts = Counter(_observation_key(record) for record in all_data)
    current_key_counts = Counter(_observation_key(record) for record in current)
    all_data_by_key = {
        _observation_key(record): record for record in all_data
    }
    current_by_key = {
        _observation_key(record): record for record in current
    }
    overlap_keys = all_data_key_counts.keys() & current_key_counts.keys()
    differing_overlap_records = [
        {
            "key": list(key),
            "all_data": all_data_by_key[key],
            "current": current_by_key[key],
        }
        for key in overlap_keys
        if all_data_by_key[key] != current_by_key[key]
    ]
    differing_overlap_examples = [
        *differing_overlap_records[:TYPE_SAMPLE_LIMIT]
    ]

    period_counts = Counter(record.get("period", "") for record in all_data)
    expected_periods = sorted(period_lookup)
    unexpected_periods = sorted(set(period_counts) - set(expected_periods))
    years = [
        int(record["year"])
        for record in all_data
        if record.get("year", "").isdigit()
    ]

    value_counts: Counter[str] = Counter(
        {
            "blank_or_null": 0,
            "non_numeric": 0,
            "non_finite": 0,
            "negative": 0,
            "zero": 0,
            "positive": 0,
        }
    )
    numeric_values: list[float] = []
    numeric_observations: list[dict[str, Any]] = []
    for record in all_data:
        raw_value = record.get("value", "")
        if not raw_value:
            value_counts["blank_or_null"] += 1
            continue
        try:
            numeric_value = float(raw_value)
        except ValueError:
            value_counts["non_numeric"] += 1
            continue
        if not math.isfinite(numeric_value):
            value_counts["non_finite"] += 1
            continue
        numeric_values.append(numeric_value)
        numeric_observations.append(
            {
                "series_id": record.get("series_id"),
                "year": record.get("year"),
                "period": record.get("period"),
                "value": numeric_value,
            }
        )
        if numeric_value < 0:
            value_counts["negative"] += 1
        elif numeric_value == 0:
            value_counts["zero"] += 1
        else:
            value_counts["positive"] += 1

    footnote_counts: Counter[str] = Counter()
    for record in all_data:
        for code in filter(None, (part.strip() for part in record.get(
            "footnote_codes", ""
        ).split(","))):
            footnote_counts[code] += 1

    population_records = population_payload["data"]
    population_years = [
        record["Year"]
        for record in population_records
        if isinstance(record.get("Year"), int)
        and not isinstance(record.get("Year"), bool)
    ]
    population_year_counts = Counter(population_years)
    duplicate_population_years = {
        str(year): count
        for year, count in sorted(population_year_counts.items())
        if count > 1
    }
    missing_population_years = []
    if population_years:
        missing_population_years = sorted(
            set(range(min(population_years), max(population_years) + 1))
            - set(population_years)
        )
    invalid_population_records = [
        index
        for index, record in enumerate(population_records)
        if not isinstance(record.get("Population"), (int, float))
        or isinstance(record.get("Population"), bool)
        or not math.isfinite(record["Population"])
        or record["Population"] <= 0
    ]

    documentation_path = bls_dir / "pr.txt"
    documentation = documentation_path.read_text(
        encoding="utf-8", errors="replace"
    )
    documentation_lower = documentation.casefold()
    normalized_documentation = " ".join(documentation_lower.split())
    section_numbers = sorted(
        {
            int(match)
            for match in re.findall(
                r"(?mi)^\s*section\s+([3-7])\s*$", documentation
            )
        }
    )
    documentation_findings = {
        "path": "bls/pr.txt",
        "sections_found": section_numbers,
        "tab_separated_headers_documented": (
            "data elements are separated by tabs" in normalized_documentation
        ),
        "all_data_file_documented": (
            "pr.data.1.alldata" in normalized_documentation
            and "all data" in normalized_documentation
        ),
        "current_file_documented_as_ytd": (
            "pr.data.0.current" in normalized_documentation
            and "current year-to-date data" in normalized_documentation
        ),
        "annual_average_period_documented": (
            "q05" in normalized_documentation
            and "annual average" in normalized_documentation
        ),
        "series_id_code_components_documented": all(
            term in normalized_documentation
            for term in (
                "sector_code",
                "class_code",
                "measure_code",
                "duration_code",
                "seasonal",
            )
        ),
    }

    return {
        "bls": {
            "duplicate_observation_keys": {
                "key_fields": ["series_id", "year", "period"],
                "all_data": _duplicate_key_profile(all_data),
                "current": _duplicate_key_profile(current),
            },
            "current_all_data_overlap": {
                "distinct_overlapping_key_count": len(overlap_keys),
                "current_rows_in_overlap": sum(
                    current_key_counts[key] for key in overlap_keys
                ),
                "all_data_rows_in_overlap": sum(
                    all_data_key_counts[key] for key in overlap_keys
                ),
                "identical_full_record_count": sum(
                    all_data_by_key[key] == current_by_key[key]
                    for key in overlap_keys
                ),
                "different_full_record_count": len(differing_overlap_records),
                "different_record_examples": differing_overlap_examples,
                "recommendation": (
                    "Use pr.data.1.AllData as the fact source; do not union "
                    "pr.data.0.Current because it is a current-year-to-date "
                    "subset and overlapping records would be duplicated."
                ),
            },
            "series_referential_integrity": {
                "series_metadata_rows": len(series),
                "all_data_distinct_series_ids": len(all_data_series_ids),
                "missing_series_id_count": len(missing_series_ids),
                "missing_series_id_examples": missing_series_ids[
                    :TYPE_SAMPLE_LIMIT
                ],
                "current_missing_series_id_count": len(current_missing_series_ids),
                "current_missing_series_id_examples": current_missing_series_ids[
                    :TYPE_SAMPLE_LIMIT
                ],
            },
            "series_lookup_integrity": series_lookup_integrity,
            "target_series": {
                "series_id": target_series_id,
                "exists_in_metadata": target_metadata is not None,
                "metadata": target_metadata,
                "description_lookup_fields": [
                    "sector_code",
                    "class_code",
                    "measure_code",
                    "duration_code",
                    "seasonal",
                ],
                "constructed_description": target_description,
                "all_data_record_count": len(target_records),
                "q01_record_count": len(target_q01_records),
                "q01_year_range": (
                    [
                        min(int(record["year"]) for record in target_q01_records),
                        max(int(record["year"]) for record in target_q01_records),
                    ]
                    if target_q01_records
                    else None
                ),
                "q01_examples": target_q01_records[:TYPE_SAMPLE_LIMIT],
            },
            "periods": {
                "observed_counts": dict(sorted(period_counts.items())),
                "valid_period_names": period_lookup,
                "unexpected_periods": unexpected_periods,
                "quarterly_periods": [
                    period for period in ("Q01", "Q02", "Q03", "Q04")
                    if period in period_lookup
                ],
                "annual_average_period": (
                    "Q05" if period_lookup.get("Q05") == "Annual Average" else None
                ),
            },
            "year_range": [min(years), max(years)] if years else None,
            "value_quality": {
                "counts": dict(sorted(value_counts.items())),
                "minimum": min(numeric_values) if numeric_values else None,
                "maximum": max(numeric_values) if numeric_values else None,
                "footnote_code_counts": dict(sorted(footnote_counts.items())),
                "footnote_code_descriptions": {
                    code: footnote_lookup.get(code)
                    for code in sorted(footnote_counts)
                },
                "largest_absolute_value_examples": sorted(
                    numeric_observations,
                    key=lambda observation: abs(observation["value"]),
                    reverse=True,
                )[:TYPE_SAMPLE_LIMIT],
            },
        },
        "datausa": {
            "record_count": len(population_records),
            "year_counts": {
                str(year): count
                for year, count in sorted(population_year_counts.items())
            },
            "duplicate_years": duplicate_population_years,
            "year_range": (
                [min(population_years), max(population_years)]
                if population_years
                else None
            ),
            "missing_years_within_range": missing_population_years,
            "invalid_or_nonpositive_population_record_indexes": (
                invalid_population_records
            ),
            "population_minimum": min(
                record["Population"] for record in population_records
            ) if population_records else None,
            "population_maximum": max(
                record["Population"] for record in population_records
            ) if population_records else None,
        },
        "source_documentation": documentation_findings,
    }


def _write_download(
    session: requests.Session,
    url: str,
    destination: Path,
    timeout: tuple[int, int],
) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(f"{destination.name}.part")
    digest = hashlib.sha256()
    size_bytes = 0
    prefix = bytearray()

    response = session.get(url, timeout=timeout, stream=True)
    try:
        response.raise_for_status()
        with temporary_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                if len(prefix) < 512:
                    prefix.extend(chunk[: 512 - len(prefix)])
                output.write(chunk)
                digest.update(chunk)
                size_bytes += len(chunk)

        if size_bytes == 0:
            raise ValueError(f"Source returned an empty file: {url}")
        if url == BLS_DIRECTORY_URL or (
            prefix.lstrip().lower().startswith((b"<!doctype html", b"<html", b"<head"))
        ):
            raise ValueError(f"Expected a data file but received HTML: {url}")
        os.replace(temporary_path, destination)
    finally:
        response.close()
        temporary_path.unlink(missing_ok=True)

    return {
        "url": url,
        "downloaded_at": utc_timestamp(),
        "size_bytes": size_bytes,
        "sha256": digest.hexdigest(),
    }


def download_and_profile(
    output_dir: Path,
    contact_email: str,
    timeout: tuple[int, int] = (10, 120),
    session: requests.Session | None = None,
) -> dict[str, Any]:
    if not contact_email or "@" not in contact_email:
        raise ValueError("Provide a valid BLS contact email.")

    owns_session = session is None
    http = session or requests.Session()
    if owns_session:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        http.mount("https://", HTTPAdapter(max_retries=retry))
        http.mount("http://", HTTPAdapter(max_retries=retry))
    http.headers.update(
        {
            "User-Agent": f"rearc-databricks-quest-source-profiler/0.1 (contact: {contact_email})",
            "Accept": "text/html, text/plain, application/json, */*",
        }
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        listing = http.get(BLS_DIRECTORY_URL, timeout=timeout)
        try:
            listing.raise_for_status()
            file_urls = discover_file_urls(listing.text)
        finally:
            listing.close()
        if not file_urls:
            raise RuntimeError("No BLS files were found in the directory listing.")

        bls_profiles: list[dict[str, Any]] = []
        for url in file_urls:
            filename = unquote(PurePosixPath(urlparse(url).path).name)
            destination = output_dir / "bls" / filename
            metadata = _write_download(http, url, destination, timeout)
            bls_profiles.append(
                {
                    "filename": filename,
                    "download": metadata,
                    "profile": profile_delimited_file(destination),
                }
            )
            LOGGER.info("Downloaded and profiled BLS file %s", filename)

        population_path = output_dir / "population" / "population.json"
        population_download = _write_download(
            http, POPULATION_API_URL, population_path, timeout
        )
        with population_path.open("r", encoding="utf-8") as source:
            population_payload = json.load(source)
        validate_population_payload(population_payload)

        report = {
            "generated_at": utc_timestamp(),
            "source_urls": {
                "bls_directory": BLS_DIRECTORY_URL,
                "datausa": POPULATION_API_URL,
            },
            "bls_files_discovered": len(bls_profiles),
            "bls_files": bls_profiles,
            "population": {
                "filename": "population/population.json",
                "download": population_download,
                "profile": profile_json_payload(population_payload),
                "response_keys": sorted(population_payload),
            },
            "additional_checks": profile_additional_checks(
                output_dir / "bls", population_payload
            ),
        }
        report_path = output_dir / "profile_report.json"
        temporary_report = report_path.with_name(f"{report_path.name}.tmp")
        try:
            temporary_report.write_text(
                json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary_report, report_path)
        finally:
            temporary_report.unlink(missing_ok=True)
        LOGGER.info("Profile report written to %s", report_path)
        return report
    finally:
        if owns_session:
            http.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download the current BLS productivity directory and DataUSA population "
            "response locally, then profile source columns and data quality."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data") / "source_inspection",
        help="Local output directory (default: data/source_inspection)",
    )
    parser.add_argument(
        "--contact-email",
        default=os.environ.get("BLS_CONTACT_EMAIL", ""),
        help="Contact email for the BLS User-Agent (or set BLS_CONTACT_EMAIL)",
    )
    parser.add_argument("--connect-timeout", type=int, default=10)
    parser.add_argument("--read-timeout", type=int, default=120)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        report = download_and_profile(
            args.output_dir,
            args.contact_email,
            timeout=(args.connect_timeout, args.read_timeout),
        )
    except (OSError, requests.RequestException, ValueError, RuntimeError) as error:
        LOGGER.error("Source download/profile failed: %s", error)
        return 1

    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir.resolve()),
                "bls_files_discovered": report["bls_files_discovered"],
                "population_records": report["population"]["profile"]["row_count"],
                "report": str((args.output_dir / "profile_report.json").resolve()),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
