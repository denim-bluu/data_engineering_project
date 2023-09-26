import argparse
import os
from datetime import timedelta

import loguru
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine

logger = loguru.logger


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url):
    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df


@task(log_prints=True)
def transform_data(df) -> pd.DataFrame:
    logger.info(
        f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}"
    )
    df = df[df["passenger_count"] != 0]
    logger.info(
        f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}"
    )
    return df


@task(log_prints=True, retries=3)
def ingest_data(
    user: str,
    password: str,
    host: str,
    port: str,
    db: str,
    table_name: str,
    df: pd.DataFrame,
):
    pg_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(pg_url)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Ingest Flow")
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    raw_df = extract_data(url)
    processed_df = transform_data(raw_df)
    ingest_data(
        user,
        password,
        host,
        port,
        db,
        table_name,
        processed_df,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()
    main(args)
