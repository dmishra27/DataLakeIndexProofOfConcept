from __future__ import annotations

import unittest

from dagster_project.assets import ALL_ASSETS
from dagster_project.definitions import defs, rebuild_lakehouse_job


class DefinitionsTests(unittest.TestCase):
    def test_all_assets_are_registered(self) -> None:
        self.assertEqual(len(ALL_ASSETS), 6)

    def test_resolved_job_has_expected_name(self) -> None:
        self.assertEqual(rebuild_lakehouse_job.name, "rebuild_lakehouse")

    def test_definitions_can_resolve_job(self) -> None:
        self.assertEqual(defs.resolve_job_def("rebuild_lakehouse").name, "rebuild_lakehouse")


if __name__ == "__main__":
    unittest.main()
