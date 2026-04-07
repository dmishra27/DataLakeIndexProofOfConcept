from __future__ import annotations

from .source_data import norm, source_files


def yellow_query() -> str:
    """Return the DuckDB query that normalizes yellow taxi trips into the fact shape."""
    file_list = ", ".join(repr(norm(path)) for path in source_files("yellow"))
    return f"""
        SELECT
            CAST(VendorID AS INTEGER) AS vendor_id,
            tpep_pickup_datetime AS pickup_at,
            tpep_dropoff_datetime AS dropoff_at,
            CAST(strftime(tpep_pickup_datetime, '%G') AS INTEGER) AS pickup_year,
            CAST(strftime(tpep_pickup_datetime, '%m') AS INTEGER) AS pickup_month,
            CAST(date_trunc('day', tpep_pickup_datetime) AS DATE) AS pickup_date,
            CAST(date_trunc('day', tpep_dropoff_datetime) AS DATE) AS dropoff_date,
            CAST(EXTRACT('hour' FROM tpep_pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(passenger_count AS BIGINT) AS passenger_count,
            trip_distance,
            CAST(RatecodeID AS INTEGER) AS rate_code_id,
            store_and_fwd_flag,
            CAST(PULocationID AS INTEGER) AS pickup_location_id,
            CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
            CAST(payment_type AS INTEGER) AS payment_type_id,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee AS airport_fee,
            cbd_congestion_fee,
            strftime(tpep_pickup_datetime, '%Y-%m') AS service_month,
            'yellow' AS service_type
        FROM read_parquet([{file_list}])
    """


def green_query() -> str:
    """Return the DuckDB query that normalizes green taxi trips into the fact shape."""
    file_list = ", ".join(repr(norm(path)) for path in source_files("green"))
    return f"""
        SELECT
            CAST(VendorID AS INTEGER) AS vendor_id,
            lpep_pickup_datetime AS pickup_at,
            lpep_dropoff_datetime AS dropoff_at,
            CAST(strftime(lpep_pickup_datetime, '%G') AS INTEGER) AS pickup_year,
            CAST(strftime(lpep_pickup_datetime, '%m') AS INTEGER) AS pickup_month,
            CAST(date_trunc('day', lpep_pickup_datetime) AS DATE) AS pickup_date,
            CAST(date_trunc('day', lpep_dropoff_datetime) AS DATE) AS dropoff_date,
            CAST(EXTRACT('hour' FROM lpep_pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(passenger_count AS BIGINT) AS passenger_count,
            trip_distance,
            CAST(RatecodeID AS INTEGER) AS rate_code_id,
            store_and_fwd_flag,
            CAST(PULocationID AS INTEGER) AS pickup_location_id,
            CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
            CAST(payment_type AS INTEGER) AS payment_type_id,
            CAST(trip_type AS INTEGER) AS trip_type_id,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            cbd_congestion_fee,
            strftime(lpep_pickup_datetime, '%Y-%m') AS service_month,
            'green' AS service_type
        FROM read_parquet([{file_list}])
    """


def fhv_query() -> str:
    """Return the DuckDB query that normalizes FHV trips into the fact shape."""
    file_list = ", ".join(repr(norm(path)) for path in source_files("fhv"))
    return f"""
        SELECT
            dispatching_base_num AS dispatching_base_number,
            Affiliated_base_number AS affiliated_base_number,
            pickup_datetime AS pickup_at,
            dropOff_datetime AS dropoff_at,
            CAST(strftime(pickup_datetime, '%G') AS INTEGER) AS pickup_year,
            CAST(strftime(pickup_datetime, '%m') AS INTEGER) AS pickup_month,
            CAST(date_trunc('day', pickup_datetime) AS DATE) AS pickup_date,
            CAST(date_trunc('day', dropOff_datetime) AS DATE) AS dropoff_date,
            CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(PUlocationID AS INTEGER) AS pickup_location_id,
            CAST(DOlocationID AS INTEGER) AS dropoff_location_id,
            CAST(SR_Flag AS INTEGER) AS sr_flag,
            strftime(pickup_datetime, '%Y-%m') AS service_month,
            'fhv' AS service_type
        FROM read_parquet([{file_list}])
    """


def trip_query() -> str:
    """Return the DuckDB query that unions yellow, green, and FHV trips into one fact shape."""
    yellow_files = ", ".join(repr(norm(path)) for path in source_files("yellow"))
    green_files = ", ".join(repr(norm(path)) for path in source_files("green"))
    fhv_files = ", ".join(repr(norm(path)) for path in source_files("fhv"))
    return f"""
        SELECT
            CAST(VendorID AS INTEGER) AS vendor_id,
            CAST(NULL AS VARCHAR) AS dispatching_base_number,
            CAST(NULL AS VARCHAR) AS affiliated_base_number,
            tpep_pickup_datetime AS pickup_at,
            tpep_dropoff_datetime AS dropoff_at,
            CAST(strftime(tpep_pickup_datetime, '%G') AS INTEGER) AS pickup_year,
            CAST(strftime(tpep_pickup_datetime, '%m') AS INTEGER) AS pickup_month,
            CAST(date_trunc('day', tpep_pickup_datetime) AS DATE) AS pickup_date,
            CAST(date_trunc('day', tpep_dropoff_datetime) AS DATE) AS dropoff_date,
            CAST(EXTRACT('hour' FROM tpep_pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(passenger_count AS BIGINT) AS passenger_count,
            trip_distance,
            CAST(RatecodeID AS INTEGER) AS rate_code_id,
            store_and_fwd_flag,
            CAST(PULocationID AS INTEGER) AS pickup_location_id,
            CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
            CAST(payment_type AS INTEGER) AS payment_type_id,
            CAST(NULL AS INTEGER) AS trip_type_id,
            CAST(NULL AS INTEGER) AS sr_flag,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            CAST(NULL AS DOUBLE) AS ehail_fee,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee AS airport_fee,
            cbd_congestion_fee,
            strftime(tpep_pickup_datetime, '%Y-%m') AS service_month,
            'yellow' AS service_type
        FROM read_parquet([{yellow_files}])

        UNION ALL

        SELECT
            CAST(VendorID AS INTEGER) AS vendor_id,
            CAST(NULL AS VARCHAR) AS dispatching_base_number,
            CAST(NULL AS VARCHAR) AS affiliated_base_number,
            lpep_pickup_datetime AS pickup_at,
            lpep_dropoff_datetime AS dropoff_at,
            CAST(strftime(lpep_pickup_datetime, '%G') AS INTEGER) AS pickup_year,
            CAST(strftime(lpep_pickup_datetime, '%m') AS INTEGER) AS pickup_month,
            CAST(date_trunc('day', lpep_pickup_datetime) AS DATE) AS pickup_date,
            CAST(date_trunc('day', lpep_dropoff_datetime) AS DATE) AS dropoff_date,
            CAST(EXTRACT('hour' FROM lpep_pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(passenger_count AS BIGINT) AS passenger_count,
            trip_distance,
            CAST(RatecodeID AS INTEGER) AS rate_code_id,
            store_and_fwd_flag,
            CAST(PULocationID AS INTEGER) AS pickup_location_id,
            CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
            CAST(payment_type AS INTEGER) AS payment_type_id,
            CAST(trip_type AS INTEGER) AS trip_type_id,
            CAST(NULL AS INTEGER) AS sr_flag,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            CAST(NULL AS DOUBLE) AS airport_fee,
            cbd_congestion_fee,
            strftime(lpep_pickup_datetime, '%Y-%m') AS service_month,
            'green' AS service_type
        FROM read_parquet([{green_files}])

        UNION ALL

        SELECT
            CAST(NULL AS INTEGER) AS vendor_id,
            dispatching_base_num AS dispatching_base_number,
            Affiliated_base_number AS affiliated_base_number,
            pickup_datetime AS pickup_at,
            dropOff_datetime AS dropoff_at,
            CAST(strftime(pickup_datetime, '%G') AS INTEGER) AS pickup_year,
            CAST(strftime(pickup_datetime, '%m') AS INTEGER) AS pickup_month,
            CAST(date_trunc('day', pickup_datetime) AS DATE) AS pickup_date,
            CAST(date_trunc('day', dropOff_datetime) AS DATE) AS dropoff_date,
            CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS pickup_hour,
            CAST(NULL AS BIGINT) AS passenger_count,
            CAST(NULL AS DOUBLE) AS trip_distance,
            CAST(NULL AS INTEGER) AS rate_code_id,
            CAST(NULL AS VARCHAR) AS store_and_fwd_flag,
            CAST(PUlocationID AS INTEGER) AS pickup_location_id,
            CAST(DOlocationID AS INTEGER) AS dropoff_location_id,
            CAST(NULL AS INTEGER) AS payment_type_id,
            CAST(NULL AS INTEGER) AS trip_type_id,
            CAST(SR_Flag AS INTEGER) AS sr_flag,
            CAST(NULL AS DOUBLE) AS fare_amount,
            CAST(NULL AS DOUBLE) AS extra,
            CAST(NULL AS DOUBLE) AS mta_tax,
            CAST(NULL AS DOUBLE) AS tip_amount,
            CAST(NULL AS DOUBLE) AS tolls_amount,
            CAST(NULL AS DOUBLE) AS ehail_fee,
            CAST(NULL AS DOUBLE) AS improvement_surcharge,
            CAST(NULL AS DOUBLE) AS total_amount,
            CAST(NULL AS DOUBLE) AS congestion_surcharge,
            CAST(NULL AS DOUBLE) AS airport_fee,
            CAST(NULL AS DOUBLE) AS cbd_congestion_fee,
            strftime(pickup_datetime, '%Y-%m') AS service_month,
            'fhv' AS service_type
        FROM read_parquet([{fhv_files}])
    """
