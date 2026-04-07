from dagster import Definitions, define_asset_job

from .assets import ALL_ASSETS

_rebuild_lakehouse_job = define_asset_job("rebuild_lakehouse")


defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[_rebuild_lakehouse_job],
)

rebuild_lakehouse_job = defs.resolve_job_def("rebuild_lakehouse")
