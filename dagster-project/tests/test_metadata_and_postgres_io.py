from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_project import metadata_io, postgres_io


class FakeAddActions:
    def __init__(self) -> None:
        self.schema = type(
            "Schema",
            (),
            {
                "names": [
                    "path",
                    "size_bytes",
                    "modification_time",
                    "data_change",
                    "num_records",
                    "null_count.vendor_id",
                    "min.vendor_id",
                    "max.vendor_id",
                    "null_count.pickup_at",
                    "min.pickup_at",
                    "max.pickup_at",
                ]
            },
        )()

    def to_pylist(self) -> list[dict[str, object]]:
        return [
            {
                "path": "part-00001.snappy.parquet",
                "size_bytes": 123,
                "modification_time": datetime(2026, 4, 4, 12, 0, 0),
                "data_change": True,
                "num_records": 10,
                "null_count.vendor_id": 1,
                "min.vendor_id": 1,
                "max.vendor_id": 2,
                "null_count.pickup_at": 0,
                "min.pickup_at": datetime(2026, 4, 1, 0, 0, 0),
                "max.pickup_at": datetime(2026, 4, 1, 1, 0, 0),
            }
        ]


class FakeDeltaTable:
    def get_add_actions(self, flatten: bool = True) -> FakeAddActions:
        self.flatten = flatten
        return FakeAddActions()


class FakeSnapshot:
    def __init__(self, relative_paths: list[str], absolute_paths: list[str]) -> None:
        self._relative_paths = relative_paths
        self._absolute_paths = absolute_paths

    def files(self) -> list[str]:
        return self._relative_paths

    def file_uris(self) -> list[str]:
        return self._absolute_paths


class MetadataIOTests(unittest.TestCase):
    def test_stringify_stat_value_handles_dates_and_none(self) -> None:
        self.assertEqual(metadata_io._stringify_stat_value(date(2026, 4, 4)), "2026-04-04")
        self.assertIsNone(metadata_io._stringify_stat_value(None))
        self.assertEqual(metadata_io._stringify_stat_value(5), "5")

    @patch.object(metadata_io, "delta_table", return_value=FakeDeltaTable())
    def test_collect_current_snapshot_file_stats_flattens_stats(self, _mock_delta_table) -> None:
        dataframe = metadata_io.collect_current_snapshot_file_stats(["fact_trip_yellow"])
        self.assertEqual(len(dataframe), 2)
        self.assertEqual(set(dataframe["column_name"]), {"vendor_id", "pickup_at"})
        self.assertIn("min_value_text", dataframe.columns)

    @patch.object(metadata_io, "delta_table", return_value=FakeDeltaTable())
    def test_collect_current_snapshot_files_builds_json_payload(self, _mock_delta_table) -> None:
        dataframe = metadata_io.collect_current_snapshot_files(["fact_trip_yellow"])
        self.assertEqual(len(dataframe), 1)
        payload = json.loads(dataframe.iloc[0]["column_stats_json"])
        self.assertEqual(payload["vendor_id"]["null_count"], 1)
        self.assertEqual(payload["pickup_at"]["min"], "2026-04-01T00:00:00")

    def test_collect_row_group_statistics_reads_parquet_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "pickup_year=2025" / "pickup_week=5" / "part-0001.parquet"
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.table(
                {
                    "fare_amount": pa.array([3.25, 7.5, None, 11.25], type=pa.float64()),
                    "pickup_date": pa.array(
                        [
                            date(2025, 2, 1),
                            date(2025, 2, 1),
                            date(2025, 2, 2),
                            date(2025, 2, 2),
                        ],
                        type=pa.date32(),
                    ),
                }
            )
            pq.write_table(table, parquet_path, row_group_size=2)
            snapshot = FakeSnapshot(
                ["pickup_year=2025/pickup_week=5/part-0001.parquet"],
                [str(parquet_path).replace("\\", "/")],
            )
            with patch.object(metadata_io, "delta_table", return_value=snapshot):
                dataframe = metadata_io._collect_row_group_statistics(["fact_trip_yellow"])

        self.assertEqual(set(dataframe["row_group_index"]), {0, 1})
        self.assertEqual(
            dataframe.iloc[0]["absolute_file_path"],
            str(parquet_path).replace("\\", "/"),
        )
        fare_rows = dataframe[dataframe["column_name"] == "fare_amount"].sort_values("row_group_index")
        self.assertEqual(fare_rows.iloc[0]["min_value_text"], "3.25")
        self.assertEqual(fare_rows.iloc[1]["null_count"], 1)

    def test_collect_predicate_row_group_index_parses_typed_bounds(self) -> None:
        raw_frame = pd.DataFrame(
            [
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_year=2025/pickup_week=5/part-0001.parquet",
                    "absolute_file_path": "C:/warehouse/fact_trip_yellow/part-0001.parquet",
                    "row_group_index": 0,
                    "num_rows": 10,
                    "total_byte_size": 512,
                    "column_name": "fare_amount",
                    "data_type": "double",
                    "null_count": 1,
                    "min_value_text": "3.25",
                    "max_value_text": "98.50",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_year=2025/pickup_week=5/part-0001.parquet",
                    "absolute_file_path": "C:/warehouse/fact_trip_yellow/part-0001.parquet",
                    "row_group_index": 0,
                    "num_rows": 10,
                    "total_byte_size": 512,
                    "column_name": "pickup_at",
                    "data_type": "timestamp[us]",
                    "null_count": 0,
                    "min_value_text": "2025-02-01T00:00:00",
                    "max_value_text": "2025-02-01T00:59:59",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_year=2025/pickup_week=5/part-0001.parquet",
                    "absolute_file_path": "C:/warehouse/fact_trip_yellow/part-0001.parquet",
                    "row_group_index": 0,
                    "num_rows": 10,
                    "total_byte_size": 512,
                    "column_name": "pickup_date",
                    "data_type": "date32[day]",
                    "null_count": 0,
                    "min_value_text": "2025-02-01",
                    "max_value_text": "2025-02-01",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_year=2025/pickup_week=5/part-0001.parquet",
                    "absolute_file_path": "C:/warehouse/fact_trip_yellow/part-0001.parquet",
                    "row_group_index": 0,
                    "num_rows": 10,
                    "total_byte_size": 512,
                    "column_name": "store_and_fwd_flag",
                    "data_type": "bool",
                    "null_count": 3,
                    "min_value_text": "false",
                    "max_value_text": "true",
                },
            ]
        )

        with patch.object(metadata_io, "_collect_row_group_statistics", return_value=raw_frame):
            result = metadata_io.collect_predicate_row_group_index(["fact_trip_yellow"])

        fare_row = result[result["column_name"] == "fare_amount"].iloc[0]
        self.assertEqual(str(fare_row["min_numeric"]), "3.25")
        self.assertEqual(str(fare_row["max_numeric"]), "98.50")
        pickup_at_row = result[result["column_name"] == "pickup_at"].iloc[0]
        self.assertEqual(pickup_at_row["min_timestamp"], datetime(2025, 2, 1, 0, 0, 0))
        pickup_date_row = result[result["column_name"] == "pickup_date"].iloc[0]
        self.assertEqual(pickup_date_row["min_date"], date(2025, 2, 1))
        flag_row = result[result["column_name"] == "store_and_fwd_flag"].iloc[0]
        self.assertEqual(flag_row["min_boolean"], False)
        self.assertTrue(flag_row["has_nulls"])
        self.assertEqual(flag_row["row_group_index"], 0)


class PostgresIOTests(unittest.TestCase):
    def test_quoted_identifier_escapes_quotes(self) -> None:
        self.assertEqual(postgres_io.quoted_identifier('a"b'), '"a""b"')

    def test_dtype_overrides_marks_json_columns(self) -> None:
        dataframe = pd.DataFrame(
            {
                "payload_json": ["{}"],
                "min_date": [date(2025, 2, 1)],
                "min_boolean": [True],
                "min_numeric": ["3.25"],
                "plain_text": ["x"],
            }
        )
        overrides = postgres_io.dtype_overrides(dataframe)
        self.assertIn("payload_json", overrides)
        self.assertIn("min_date", overrides)
        self.assertIn("min_boolean", overrides)
        self.assertIn("min_numeric", overrides)
        self.assertNotIn("plain_text", overrides)

    def test_frame_from_batch_preserves_arrow_backed_types(self) -> None:
        batch = pa.record_batch(
            [
                pa.array([1, None], type=pa.int64()),
                pa.array([True, None], type=pa.bool_()),
            ],
            names=["value", "flag"],
        )
        dataframe = postgres_io.frame_from_batch(batch)
        self.assertIsInstance(dataframe, pl.DataFrame)
        self.assertEqual(dataframe.schema["value"], pl.Int64)
        self.assertEqual(dataframe.schema["flag"], pl.Boolean)

    @patch("dagster_project.postgres_io.time.time", return_value=110.0)
    def test_progress_message_reports_rows_batches_and_rate(self, _mock_time) -> None:
        message = postgres_io._progress_message("fact_trip", 2_000_000, 20, 100.0)
        self.assertIn("fact_trip", message)
        self.assertIn("2,000,000 rows", message)
        self.assertIn("20 batches", message)
        self.assertIn("200,000 rows/s", message)


if __name__ == "__main__":
    unittest.main()
