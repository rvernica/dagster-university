import io

import dagster as dg
import pandas as pd
import requests
from dagster_duckdb import DuckDBResource

from dagster_essentials.assets import constants
from dagster_essentials.partitions import monthly_partition


@dg.asset(partitions_def=monthly_partition, group_name="raw_files")
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet",
        timeout=10,
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)

    num_rows = len(
        pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch))
    )
    return dg.MaterializeResult(
        metadata={"Number of records": dg.MetadataValue.int(num_rows)}
    )


@dg.asset(
    group_name="raw_files",
)
def taxi_zones_file() -> dg.MaterializeResult:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.

    This asset will contain a unique identifier and name for each part of NYC as a distinct taxi zone.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv",
        timeout=10,
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

    return dg.MaterializeResult(
        metadata={
            "Number of records": dg.MetadataValue.int(
                len(pd.read_csv(io.StringIO(raw_zones.content.decode("utf-8"))))
            )
        }
    )


@dg.asset(
    deps=["taxi_trips_file"], partitions_def=monthly_partition, group_name="ingested"
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """

    month_to_fetch = context.partition_key[:-3]

    query = f"""
        create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
        );

        delete from trips where partition_date = '{month_to_fetch}';

        insert into trips
        select
        VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
        tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(deps=["taxi_zones_file"], group_name="ingested")
def taxi_zones(database: DuckDBResource) -> None:
    """
    The taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
