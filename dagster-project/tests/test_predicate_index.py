from __future__ import annotations

import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nsi.indexer import (  # noqa: E402
    Predicate,
    build_candidate_files_query,
    build_duckdb_parquet_scan,
    build_footer_row_group_stats_frame,
    build_predicate_index_frame,
    footer_row_group_stats_dtypes,
    iter_warehouse_parquet_files,
)


class PredicateIndexTests(unittest.TestCase):
    def test_build_predicate_index_frame_parses_typed_bounds(self) -> None:
        stats_frame = pd.DataFrame(
            [
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_date=2025-02-01/part-0001.parquet",
                    "column_name": "fare_amount",
                    "num_records": 10,
                    "null_count": 1,
                    "min_value_text": "3.25",
                    "max_value_text": "98.50",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_date=2025-02-01/part-0001.parquet",
                    "column_name": "pickup_at",
                    "num_records": 10,
                    "null_count": 0,
                    "min_value_text": "2025-02-01T00:00:00",
                    "max_value_text": "2025-02-01T00:59:59",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_date=2025-02-01/part-0001.parquet",
                    "column_name": "pickup_date",
                    "num_records": 10,
                    "null_count": 0,
                    "min_value_text": "2025-02-01",
                    "max_value_text": "2025-02-01",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "file_path": "pickup_date=2025-02-01/part-0001.parquet",
                    "column_name": "store_and_fwd_flag",
                    "num_records": 10,
                    "null_count": 3,
                    "min_value_text": "false",
                    "max_value_text": "true",
                },
            ]
        )
        catalog_frame = pd.DataFrame(
            [
                {"table_name": "fact_trip_yellow", "column_name": "fare_amount", "data_type": "double"},
                {
                    "table_name": "fact_trip_yellow",
                    "column_name": "pickup_at",
                    "data_type": "timestamp[us]",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "column_name": "pickup_date",
                    "data_type": "date32[day]",
                },
                {
                    "table_name": "fact_trip_yellow",
                    "column_name": "store_and_fwd_flag",
                    "data_type": "bool",
                },
            ]
        )

        result = build_predicate_index_frame(stats_frame, catalog_frame, warehouse_dir=Path("C:/warehouse"))

        fare_row = result[result["column_name"] == "fare_amount"].iloc[0]
        self.assertEqual(fare_row["min_numeric"], Decimal("3.25"))
        self.assertEqual(fare_row["max_numeric"], Decimal("98.50"))
        pickup_at_row = result[result["column_name"] == "pickup_at"].iloc[0]
        self.assertEqual(pickup_at_row["min_timestamp"], datetime(2025, 2, 1, 0, 0, 0))
        self.assertEqual(pickup_at_row["max_timestamp"], datetime(2025, 2, 1, 0, 59, 59))
        pickup_date_row = result[result["column_name"] == "pickup_date"].iloc[0]
        self.assertEqual(pickup_date_row["min_date"], date(2025, 2, 1))
        self.assertEqual(pickup_date_row["max_date"], date(2025, 2, 1))
        flag_row = result[result["column_name"] == "store_and_fwd_flag"].iloc[0]
        self.assertEqual(flag_row["min_boolean"], False)
        self.assertEqual(flag_row["max_boolean"], True)
        self.assertTrue(flag_row["has_nulls"])
        self.assertFalse(flag_row["all_nulls"])

    def test_build_candidate_files_query_uses_expected_bounds_columns(self) -> None:
        query, params = build_candidate_files_query(
            "fact_trip_yellow",
            [
                Predicate("pickup_at", ">=", "2025-02-01T00:00:00"),
                Predicate("fare_amount", "between", "5.00", "25.00"),
                Predicate("store_and_fwd_flag", "is_not_null"),
            ],
            column_types={
                "pickup_at": "timestamp[us]",
                "fare_amount": "double",
                "store_and_fwd_flag": "bool",
            },
        )

        self.assertIn("CAST(p0.max_timestamp AS TIMESTAMP) >= :predicate_0_value", query)
        self.assertIn("CAST(p1.min_numeric AS NUMERIC) <= :predicate_1_second_value", query)
        self.assertIn("CAST(p1.max_numeric AS NUMERIC) >= :predicate_1_value", query)
        self.assertIn("p2.non_null_count > 0", query)
        self.assertEqual(params["predicate_1_value"], Decimal("5.00"))
        self.assertEqual(params["predicate_1_second_value"], Decimal("25.00"))

    def test_build_duckdb_parquet_scan_normalizes_windows_paths(self) -> None:
        scan = build_duckdb_parquet_scan(
            [
                "C:\\warehouse\\fact_trip_yellow\\part-0001.parquet",
                "C:\\warehouse\\fact_trip_yellow\\part-0002.parquet",
            ]
        )
        self.assertEqual(
            scan,
            "read_parquet(['C:/warehouse/fact_trip_yellow/part-0001.parquet', "
            "'C:/warehouse/fact_trip_yellow/part-0002.parquet'])",
        )

    def test_build_footer_row_group_stats_frame_reads_typed_footer_stats(self) -> None:
        with TemporaryDirectory() as temp_dir:
            warehouse_dir = Path(temp_dir) / "warehouse"
            table_dir = warehouse_dir / "fact_trip_sample" / "pickup_year=2025" / "pickup_month=2"
            table_dir.mkdir(parents=True, exist_ok=True)
            parquet_path = table_dir / "part-0001.parquet"
            table = pa.table(
                {
                    "fare_amount": [10.5, 12.75, 300.0, 450.25],
                    "pickup_at": [
                        datetime(2025, 2, 1, 8, 0, 0),
                        datetime(2025, 2, 1, 8, 30, 0),
                        datetime(2025, 2, 1, 9, 0, 0),
                        datetime(2025, 2, 1, 9, 30, 0),
                    ],
                    "pickup_date": [
                        date(2025, 2, 1),
                        date(2025, 2, 1),
                        date(2025, 2, 1),
                        date(2025, 2, 1),
                    ],
                    "is_shared": [False, False, True, True],
                }
            )
            pq.write_table(table, parquet_path, row_group_size=2)

            result = build_footer_row_group_stats_frame([parquet_path], warehouse_dir=warehouse_dir)

        self.assertEqual(len(result), 8)
        fare_row = result[
            (result["column_name"] == "fare_amount") & (result["row_group_index"] == 0)
        ].iloc[0]
        self.assertEqual(fare_row["table_name"], "fact_trip_sample")
        self.assertEqual(fare_row["file_path"], "pickup_year=2025/pickup_month=2/part-0001.parquet")
        self.assertEqual(fare_row["min_numeric"], Decimal("10.5"))
        self.assertEqual(fare_row["max_numeric"], Decimal("12.75"))
        pickup_at_row = result[
            (result["column_name"] == "pickup_at") & (result["row_group_index"] == 1)
        ].iloc[0]
        self.assertEqual(pickup_at_row["min_timestamp"], datetime(2025, 2, 1, 9, 0, 0))
        self.assertEqual(pickup_at_row["max_timestamp"], datetime(2025, 2, 1, 9, 30, 0))
        pickup_date_row = result[
            (result["column_name"] == "pickup_date") & (result["row_group_index"] == 0)
        ].iloc[0]
        self.assertEqual(pickup_date_row["min_date"], date(2025, 2, 1))
        boolean_row = result[
            (result["column_name"] == "is_shared") & (result["row_group_index"] == 1)
        ].iloc[0]
        self.assertEqual(boolean_row["min_boolean"], True)
        self.assertEqual(boolean_row["max_boolean"], True)

    def test_iter_warehouse_parquet_files_returns_all_data_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            warehouse_dir = Path(temp_dir) / "warehouse"
            first_path = warehouse_dir / "fact_trip" / "pickup_year=2025" / "part-0001.parquet"
            second_path = warehouse_dir / "fact_trip_green" / "pickup_year=2025" / "part-0001.parquet"
            first_path.parent.mkdir(parents=True, exist_ok=True)
            second_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"value": [1]}), first_path)
            pq.write_table(pa.table({"value": [2]}), second_path)

            result = iter_warehouse_parquet_files(warehouse_dir)

        self.assertEqual(result, [first_path, second_path])

    def test_footer_row_group_stats_dtypes_marks_typed_bounds(self) -> None:
        overrides = footer_row_group_stats_dtypes()
        self.assertIn("min_numeric", overrides)
        self.assertIn("max_timestamp", overrides)
        self.assertIn("min_date", overrides)
        self.assertIn("max_boolean", overrides)


if __name__ == "__main__":
    unittest.main()
