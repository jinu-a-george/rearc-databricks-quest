from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.ingestion.local_source_profiler import (
    profile_additional_checks,
    profile_delimited_file,
    profile_json_payload,
)


class DelimitedFileProfileTests(unittest.TestCase):
    def test_profiles_tab_delimited_headers_types_and_missing_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_path = Path(temporary_directory) / "sample.tsv"
            source_path.write_text(
                "year\tvalue\tlabel\n"
                "2020\t1.25\talpha\n"
                "2021\tNA\t \n"
                "2022\tbad\tgamma\n",
                encoding="utf-8",
            )

            profile = profile_delimited_file(source_path)

        self.assertEqual(profile["delimiter"], "tab")
        self.assertEqual(profile["header"], ["year", "value", "label"])
        self.assertEqual(profile["row_count"], 3)
        self.assertEqual(profile["columns"][0]["inferred_type"], "integer")
        self.assertEqual(
            profile["columns"][1]["missing_token_counts"], {"na": 1}
        )
        self.assertEqual(profile["columns"][1]["suspected_type_mismatch_count"], 1)
        self.assertEqual(profile["columns"][2]["blank_count"], 1)
        self.assertEqual(profile["malformed_row_count"], 0)

    def test_reports_irregular_field_counts_as_malformed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_path = Path(temporary_directory) / "sample.tsv"
            source_path.write_text(
                "id\tcount\none\t1\ntwo\t2\textra\n", encoding="utf-8"
            )
            profile = profile_delimited_file(source_path)

        self.assertEqual(profile["malformed_row_count"], 1)
        self.assertEqual(profile["malformed_row_samples"][0]["actual_fields"], 3)

    def test_classifies_non_delimited_document_as_plain_text(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_path = Path(temporary_directory) / "notes.txt"
            source_path.write_text(
                "first line, with a comma\n"
                "second line: punctuation; is not a delimiter\n"
                "third plain line\n",
                encoding="utf-8",
            )
            profile = profile_delimited_file(source_path)

        self.assertEqual(profile["file_type"], "plain_text")
        self.assertIsNone(profile["delimiter"])
        self.assertEqual(profile["row_count"], 3)


class JsonProfileTests(unittest.TestCase):
    def test_profiles_datausa_fields_and_null_values(self) -> None:
        payload = {
            "columns": ["Year", "Population", "Nation"],
            "data": [
                {"Year": 2020, "Population": 300.0, "Nation": "United States"},
                {"Year": 2021, "Population": None, "Nation": ""},
            ],
        }
        profile = profile_json_payload(payload)

        self.assertEqual(profile["file_type"], "json_records")
        self.assertEqual(profile["row_count"], 2)
        self.assertEqual(profile["columns"][0]["inferred_type"], "integer")
        self.assertEqual(
            profile["columns"][1]["missing_token_counts"], {"JSON null": 1}
        )
        self.assertEqual(profile["columns"][2]["blank_count"], 1)
        json.dumps(profile)


class AdditionalChecksTests(unittest.TestCase):
    def test_profiles_duplicates_overlap_metadata_and_population_year_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            bls_dir = Path(temporary_directory) / "bls"
            bls_dir.mkdir()
            files = {
                "pr.data.1.AllData": (
                    "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
                    "PRS30006032\t2020\tQ01\t-1.0\tR\n"
                    "PRS30006032\t2020\tQ01\t-1.0\tR\n"
                    "PRS30006032\t2020\tQ05\t0\t\n"
                ),
                "pr.data.0.Current": (
                    "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
                    "PRS30006032\t2020\tQ01\t-1.0\tR\n"
                ),
                "pr.series": (
                    "series_id\tsector_code\tclass_code\tmeasure_code\t"
                    "duration_code\tseasonal\tbase_year\tfootnote_codes\t"
                    "begin_year\tbegin_period\tend_year\tend_period\n"
                    "PRS30006032\t3000\t6\t03\t2\tS\t-\t\t1987\tQ02\t2026\tQ02\n"
                ),
                "pr.period": (
                    "period\tperiod_abbr\tperiod_name\n"
                    "Q01\tQTR1\t1st Quarter\n"
                    "Q05\tAN AV\tAnnual Average\n"
                ),
                "pr.sector": "sector_code\tsector_name\n3000\tManufacturing\n",
                "pr.class": "class_code\tclass_text\n6\tAll workers\n",
                "pr.measure": "measure_code\tmeasure_text\n03\tHours worked\n",
                "pr.duration": (
                    "duration_code\tduration_text\n"
                    "2\t% Change from previous quarter\n"
                ),
                "pr.seasonal": (
                    "Seasonal_code\tSeasonal_text\nS\tSeasonally Adjusted\n"
                ),
                "pr.footnote": (
                    "footnote_code\tfootnote_text\nR\trevised\n"
                ),
                "pr.txt": (
                    "Section 3\nSection 4\nSection 5\nSection 7\n"
                    "Data elements are separated by tabs.\n"
                    "pr.data.1.AllData - All data\n"
                    "pr.data.0.Current - All current year-to-date data\n"
                    "Q05 Annual Average\n"
                    "sector_code class_code measure_code duration_code seasonal\n"
                ),
            }
            for filename, contents in files.items():
                (bls_dir / filename).write_text(contents, encoding="utf-8")

            payload = {
                "columns": ["Nation ID", "Nation", "Year", "Population"],
                "data": [
                    {
                        "Nation ID": "01000US",
                        "Nation": "United States",
                        "Year": 2020,
                        "Population": 300.0,
                    },
                    {
                        "Nation ID": "01000US",
                        "Nation": "United States",
                        "Year": 2022,
                        "Population": 320.0,
                    },
                ],
            }
            profile = profile_additional_checks(bls_dir, payload)

        bls = profile["bls"]
        self.assertEqual(
            bls["duplicate_observation_keys"]["all_data"][
                "duplicate_key_count"
            ],
            1,
        )
        self.assertEqual(
            bls["current_all_data_overlap"]["distinct_overlapping_key_count"], 1
        )
        self.assertEqual(
            bls["current_all_data_overlap"]["identical_full_record_count"], 1
        )
        self.assertEqual(
            bls["target_series"]["constructed_description"],
            "Manufacturing — All workers — Hours worked — "
            "% Change from previous quarter — Seasonally Adjusted",
        )
        self.assertEqual(bls["target_series"]["q01_record_count"], 2)
        self.assertEqual(bls["periods"]["annual_average_period"], "Q05")
        self.assertEqual(
            bls["value_quality"]["counts"]["blank_or_null"], 0
        )
        self.assertEqual(bls["value_quality"]["footnote_code_counts"], {"R": 2})
        self.assertEqual(
            bls["value_quality"]["footnote_code_descriptions"], {"R": "revised"}
        )
        self.assertEqual(
            profile["datausa"]["missing_years_within_range"], [2021]
        )
        self.assertTrue(
            profile["source_documentation"]["current_file_documented_as_ytd"]
        )
        self.assertTrue(
            profile["source_documentation"]["tab_separated_headers_documented"]
        )


if __name__ == "__main__":
    unittest.main()
