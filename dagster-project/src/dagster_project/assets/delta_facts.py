from __future__ import annotations

from dagster import MaterializeResult, MetadataValue, asset

from ..delta_io import write_delta_from_query
from ..fact_queries import fhv_query, green_query, trip_query, yellow_query
from ..settings import DATA_SOURCE_DIR, FACT_FHV, FACT_GREEN, FACT_TRIP, FACT_YELLOW
from .delta_dimensions import delta_dimensions


@asset(deps=[delta_dimensions])
def delta_facts() -> MaterializeResult:
    """Materialize the normalized fact tables into the local Delta Lake."""
    queries = {
        FACT_YELLOW: yellow_query(),
        FACT_GREEN: green_query(),
        FACT_FHV: fhv_query(),
        FACT_TRIP: trip_query(),
    }
    for table_name, query in queries.items():
        write_delta_from_query(table_name, query, partition_by=["pickup_year", "pickup_month"])
    return MaterializeResult(
        metadata={
            "fact_tables": MetadataValue.json(sorted(queries)),
            "source_dir": MetadataValue.path(str(DATA_SOURCE_DIR)),
        }
    )
