from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=2)
def fetch(dataset_url: str) -> pd.DataFrame:
    return pd.read_csv(dataset_url)


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    print(f"Data dimension: {df.shape}")

    df = df.dropna()
    df = df.drop_duplicates()
    return df


@task()
def write_local(df: pd.DataFrame, colour: str, dataset_file: str) -> Path:
    out_dir = Path(f"data/{colour}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(f"{out_dir}/{dataset_file}.parquet")
    df.to_parquet(out_path, compression="gzip")
    return out_path

@task()
def write_gcs(path:Path) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("data-engineering")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

@flow(name="Main ETL Flow")
def etl_web_to_gcs() -> None:
    """Main ETL flow"""
    colour = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df = clean(df)
    out_path = write_local(df, colour, dataset_file)
    write_gcs(out_path)


if __name__ == "__main__":
    etl_web_to_gcs()
