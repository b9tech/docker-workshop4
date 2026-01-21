#!/usr/bin/env python
# coding: utf-8

"""
Load NYC Yellow Taxi data into Postgres (dev script)

- Downloads data from DataTalksClub release
- Creates table schema
- Loads data in chunks
"""

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# ------------------------------------------------------------
# Pandas schema configuration
# ------------------------------------------------------------
DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def run():
    # ------------------------------------------------------------
    # Postgres configuration
    # ------------------------------------------------------------
    pg_user = "root"
    pg_password = "root"
    pg_host = "localhost"
    pg_port = 55432
    pg_db = "ny_taxi"
    table_name = "yellow_taxi_data"

    # ------------------------------------------------------------
    # Dataset configuration
    # ------------------------------------------------------------
    year = 2021
    month = 1
    chunksize = 100_000

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    # ------------------------------------------------------------
    # SQLAlchemy engine
    # ------------------------------------------------------------
    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )

    # ------------------------------------------------------------
    # Create table schema
    # ------------------------------------------------------------
    df_head = pd.read_csv(
        url,
        nrows=100,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
    )

    print(pd.io.sql.get_schema(df_head, name=table_name, con=engine))

    df_head.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )

    # ------------------------------------------------------------
    # Chunked ingestion
    # ------------------------------------------------------------
    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    for df_chunk in tqdm(df_iter, desc="Ingesting data"):
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

    print("Data ingestion completed successfully.")


if __name__ == "__main__":
    run()