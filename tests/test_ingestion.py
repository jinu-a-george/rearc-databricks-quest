from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.ingestion.bls_client import discover_file_urls
from src.ingestion.config import BLS_DIRECTORY_URL, IngestionConfig
from src.ingestion.ingestion import (
    SnapshotIngestionFailure,
    ingest_snapshot,
)
from src.ingestion.manifest import (
    load_latest_manifest,
    new_snapshot_id,
    read_json,
    write_json_atomic,
)
from src.ingestion.population_client import validate_population_payload
from src.ingestion.source_monitor import (
    SourceFile,
    SourceMetadata,
    unchanged_from_previous_snapshot,
)


POPULATION_PAYLOAD = {
    "annotations": {},
    "page": {},
    "columns": ["Nation ID", "Nation", "Year", "Population"],
    "data": [
        {
            "Nation ID": "01000US",
            "Nation": "United States",
            "Year": 2018,
            "Population": 327167439.0,
        }
    ],
}
BLS_CONTENT = (
    "series_id\tsector_code\tclass_code\tmeasure_code\tduration_code\t"
    "seasonal\tbase_year\tfootnote_codes\tseries_title\tbegin_year\t"
    "begin_period\tend_year\tend_period\n"
    "PRS30006032\t3000\t6\t03\t2\tS\t-\t\tTest series\t1987\tQ02\t2026\tQ02\n"
).encode()


class FakeResponse:
    def __init__(
        self,
        content: bytes = b"",
        *,
        headers: dict[str, str] | None = None,
        status_code: int = 200,
        text: str | None = None,
    ) -> None:
        self.content = content
        self.headers = headers or {}
        self.status_code = status_code
        self.text = text if text is not None else content.decode("utf-8")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int):
        yield self.content

    def close(self) -> None:
        pass


class FakeSession:
    def __init__(self, *, invalid_bls: bool = False) -> None:
        self.headers: dict[str, str] = {}
        self.get_calls: list[str] = []
        self.invalid_bls = invalid_bls
        self.contents = {
            BLS_DIRECTORY_URL + "pr.series": (
                b"<a href='pr.series'>pr.series</a>"
            ),
            "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
            "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation"
            "&locale=en&measures=Population": json.dumps(
                POPULATION_PAYLOAD
            ).encode(),
        }

    def _headers(self, url: str) -> dict[str, str]:
        content = self.contents[url]
        return {
            "ETag": f'"{len(content)}"',
            "Last-Modified": "Wed, 05 Nov 2026 15:34:59 GMT",
            "Content-Length": str(len(content)),
        }

    def get(self, url: str, **kwargs) -> FakeResponse:
        self.get_calls.append(url)
        if url == BLS_DIRECTORY_URL:
            return FakeResponse(text=self.contents[BLS_DIRECTORY_URL + "pr.series"].decode())
        content = self.contents[url]
        if self.invalid_bls and url.endswith("/pr.series"):
            content = b"<html>not a data file</html>"
        return FakeResponse(content, headers=self._headers(url))

    def head(self, url: str, **kwargs) -> FakeResponse:
        return FakeResponse(headers=self._headers(url))

    def close(self) -> None:
        pass


class IngestionConfigTests(unittest.TestCase):
    def test_explicit_contact_email_and_volume_override_environment(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BLS_CONTACT_EMAIL": "environment@example.com",
                "RAW_VOLUME_PATH": "/Volumes/catalog/schema/volume",
            },
        ):
            config = IngestionConfig.from_environment(
                "job@example.com", raw_volume_path="/tmp/volume"
            )

        self.assertEqual(config.bls_contact_email, "job@example.com")
        self.assertEqual(config.raw_volume_path, Path("/tmp/volume"))


class BLSDirectoryTests(unittest.TestCase):
    def test_discovery_keeps_files_within_source_directory(self) -> None:
        html = """
        <a href="pr.data.1.AllData">data</a>
        <a href="pr.series">series</a>
        <a href="../other/file">outside</a>
        <a href="subdirectory/">directory</a>
        <a href="https://example.com/pr.other">external</a>
        """

        self.assertEqual(
            discover_file_urls(html, BLS_DIRECTORY_URL),
            [
                BLS_DIRECTORY_URL + "pr.data.1.AllData",
                BLS_DIRECTORY_URL + "pr.series",
            ],
        )


class PopulationPayloadTests(unittest.TestCase):
    def test_accepts_expected_data_usa_response(self) -> None:
        self.assertEqual(
            validate_population_payload(POPULATION_PAYLOAD), POPULATION_PAYLOAD
        )

    def test_rejects_missing_required_column(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            validate_population_payload({"columns": ["Year"], "data": []})

    def test_rejects_invalid_record_type(self) -> None:
        payload = {
            "columns": ["Nation ID", "Nation", "Year", "Population"],
            "data": ["not-an-object"],
        }

        with self.assertRaisesRegex(ValueError, "record 0"):
            validate_population_payload(payload)


class SnapshotManifestTests(unittest.TestCase):
    def test_snapshot_id_is_fixed_width_and_utc(self) -> None:
        snapshot_id = new_snapshot_id(
            datetime(2026, 11, 5, 15, 34, 59, 478275, tzinfo=timezone.utc)
        )

        self.assertEqual(snapshot_id, "20261105153459478275")
        self.assertEqual(len(snapshot_id), 20)

    def test_json_is_written_atomically_and_latest_loads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            snapshot_id = "20261105153459478275"
            manifest = {
                "snapshot_id": snapshot_id,
                "ingestion_status": "SUCCESS",
                "files": [],
            }
            write_json_atomic(root / "manifests" / f"{snapshot_id}.json", manifest)
            write_json_atomic(
                root / "latest.json",
                {"snapshot_id": snapshot_id, "status": "SUCCESS"},
            )

            self.assertEqual(load_latest_manifest(root), manifest)
            self.assertFalse(list(root.rglob("*.tmp")))
            self.assertEqual(
                read_json(root / "latest.json")["snapshot_id"], snapshot_id
            )


class SourceMonitoringTests(unittest.TestCase):
    def test_metadata_comparison_requires_all_validators(self) -> None:
        source = SourceFile("BLS", "pr.series", BLS_DIRECTORY_URL + "pr.series")
        metadata = SourceMetadata(source, 200, '"etag"', "date", "10")
        previous = {
            "files": [
                {
                    "source_name": "BLS",
                    "source_file": "pr.series",
                    "etag": '"etag"',
                    "last_modified": "date",
                    "content_length": "10",
                }
            ]
        }

        self.assertTrue(unchanged_from_previous_snapshot([metadata], previous))
        missing_validator = SourceMetadata(source, 200, None, "date", "10")
        self.assertFalse(
            unchanged_from_previous_snapshot([missing_validator], previous)
        )


class SnapshotIngestionTests(unittest.TestCase):
    def _config(self, root: Path) -> IngestionConfig:
        return IngestionConfig(root, "test@example.com")

    def test_successful_snapshot_preserves_sources_and_audit_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = ingest_snapshot(
                self._config(root),
                session=FakeSession(),
                databricks_job_id=17,
                databricks_job_run_id=29,
            )

            self.assertEqual(result["manifest"]["ingestion_status"], "SUCCESS")
            self.assertTrue(result["process_required"])
            self.assertEqual(len(result["audit_rows"]), 2)
            self.assertTrue(
                (root / "bls" / "snapshots" / result["snapshot_id"] / "pr.series")
                .is_file()
            )
            self.assertTrue(
                (
                    root
                    / "population"
                    / "snapshots"
                    / result["snapshot_id"]
                    / "population.json"
                ).is_file()
            )
            self.assertEqual(
                load_latest_manifest(root)["snapshot_id"], result["snapshot_id"]
            )
            self.assertTrue(result["manifest"]["files"][0]["checksum_sha256"])
            self.assertEqual(
                result["audit_rows"][0]["snapshot_status"], "SUCCESS"
            )
            self.assertEqual(result["audit_rows"][0]["status"], "SUCCESS")
            self.assertEqual(
                result["audit_rows"][0]["source_snapshot_id"],
                result["snapshot_id"],
            )
            self.assertIn(
                f"/bls/snapshots/{result['snapshot_id']}/pr.series",
                result["audit_rows"][0]["source_path"],
            )

    def test_invalid_bls_content_does_not_advance_latest_pointer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with self.assertRaises(SnapshotIngestionFailure) as raised:
                ingest_snapshot(
                    self._config(root),
                    session=FakeSession(invalid_bls=True),
                )

            self.assertFalse((root / "latest.json").exists())
            failed_manifest = read_json(
                root / "manifests" / f"{raised.exception.snapshot_id}.json"
            )
            self.assertEqual(failed_manifest["ingestion_status"], "FAILED")
            self.assertIn("HTML", failed_manifest["files"][0]["error_message"])

    def test_unchanged_sources_do_not_create_a_new_authoritative_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first = ingest_snapshot(self._config(root), session=FakeSession())
            write_json_atomic(
                root / "processing" / "latest.json",
                {
                    "snapshot_id": first["snapshot_id"],
                    "status": "SUCCESS",
                    "audit_finalized": True,
                },
            )

            second = ingest_snapshot(self._config(root), session=FakeSession())

            self.assertEqual(second["manifest"]["ingestion_status"], "UNCHANGED")
            self.assertEqual(second["authoritative_snapshot_id"], first["snapshot_id"])
            self.assertFalse(second["process_required"])
            self.assertEqual(load_latest_manifest(root)["snapshot_id"], first["snapshot_id"])
            unchanged_row = next(
                row for row in second["audit_rows"] if row["source_file"] == "pr.series"
            )
            self.assertEqual(unchanged_row["status"], "UNCHANGED")
            self.assertEqual(unchanged_row["snapshot_status"], "UNCHANGED")
            self.assertEqual(
                unchanged_row["source_snapshot_id"], first["snapshot_id"]
            )


if __name__ == "__main__":
    unittest.main()
