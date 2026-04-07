from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_project import fact_queries, source_data


class SourceDataTests(unittest.TestCase):
    def test_norm_returns_posix_path(self) -> None:
        path = Path("C:\\temp\\example.parquet")
        normalized = source_data.norm(path)
        self.assertIn("/", normalized)

    def test_source_files_returns_sorted_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            (data_dir / "yellow_tripdata_2025-02.parquet").touch()
            (data_dir / "yellow_tripdata_2025-01.parquet").touch()
            with patch.object(source_data, "DATA_SOURCE_DIR", data_dir):
                result = source_data.source_files("yellow")
        self.assertEqual(
            [path.name for path in result],
            ["yellow_tripdata_2025-01.parquet", "yellow_tripdata_2025-02.parquet"],
        )

    def test_source_files_raises_when_no_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(source_data, "DATA_SOURCE_DIR", Path(temp_dir)):
                with self.assertRaises(FileNotFoundError):
                    source_data.source_files("yellow")


class FactQueryTests(unittest.TestCase):
    @patch.object(fact_queries, "source_files")
    def test_yellow_query_uses_all_source_files(self, mock_source_files) -> None:
        mock_source_files.return_value = [Path("yellow_tripdata_2025-01.parquet")]
        query = fact_queries.yellow_query()
        self.assertIn("service_type", query)
        self.assertIn("'yellow' AS service_type", query)
        self.assertIn("read_parquet", query)

    @patch.object(fact_queries, "source_files")
    def test_green_query_sets_trip_type(self, mock_source_files) -> None:
        mock_source_files.return_value = [Path("green_tripdata_2025-01.parquet")]
        query = fact_queries.green_query()
        self.assertIn("trip_type_id", query)
        self.assertIn("'green' AS service_type", query)

    @patch.object(fact_queries, "source_files")
    def test_fhv_query_uses_dispatch_columns(self, mock_source_files) -> None:
        mock_source_files.return_value = [Path("fhv_tripdata_2025-01.parquet")]
        query = fact_queries.fhv_query()
        self.assertIn("dispatching_base_number", query)
        self.assertIn("'fhv' AS service_type", query)

    @patch.object(fact_queries, "source_files")
    def test_trip_query_unions_all_service_types(self, mock_source_files) -> None:
        def fake_source_files(service_type: str) -> list[Path]:
            return [Path(f"{service_type}_tripdata_2025-01.parquet")]

        mock_source_files.side_effect = fake_source_files
        query = fact_queries.trip_query()
        self.assertIn("UNION ALL", query)
        self.assertIn("'yellow' AS service_type", query)
        self.assertIn("'green' AS service_type", query)
        self.assertIn("'fhv' AS service_type", query)
        self.assertIn("dispatching_base_number", query)


if __name__ == "__main__":
    unittest.main()
