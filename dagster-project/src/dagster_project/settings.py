from __future__ import annotations

from pathlib import Path

WORKSPACE_ROOT = Path(__file__).parents[3]
DATA_SOURCE_DIR = WORKSPACE_ROOT / "datasource" / "nyc-taxi"
DELTA_ROOT = WORKSPACE_ROOT / "delta-lake"
DELTA_WAREHOUSE_DIR = DELTA_ROOT / "warehouse"
DELTA_METADATA_DIR = DELTA_ROOT / "metadata_exports"

DIM_LOCATION = "dim_location"
DIM_VENDOR = "dim_vendor"
DIM_PAYMENT_TYPE = "dim_payment_type"
DIM_RATE_CODE = "dim_rate_code"
DIM_TRIP_TYPE = "dim_trip_type"
DIM_BASE = "dim_base"
FACT_YELLOW = "fact_trip_yellow"
FACT_GREEN = "fact_trip_green"
FACT_FHV = "fact_trip_fhv"
FACT_TRIP = "fact_trip"

DELTA_TABLES = [
    DIM_LOCATION,
    DIM_VENDOR,
    DIM_PAYMENT_TYPE,
    DIM_RATE_CODE,
    DIM_TRIP_TYPE,
    DIM_BASE,
    FACT_YELLOW,
    FACT_GREEN,
    FACT_FHV,
    FACT_TRIP,
]
