from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task()
def extract_from_gcs(colour: str, year: int, month: int) -> Path:
    gcs_path = f"data/{colour}/{colour}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("data-engineering")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_to_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("data-engineering-credential")
    df.to_gbq(
        destination_table="dev.example",
        project_id="personal-project-400317",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL Flow"""
    colour = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(colour, year, month)
    df = transform(path)
    write_to_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
