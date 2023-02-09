from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import urllib.request

@task(retries=3)
def fetch(dataset_url: str, filename: str) -> Path:
    """Read taxi data from web into pandas DataFrame"""

    file_path = f"data/{filename}.csv.gz"
    urllib.request.urlretrieve(dataset_url, file_path)

    path = Path(file_path)
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv//{dataset_file}.csv.gz"

    #https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv
    print(dataset_url)

    path = fetch(dataset_url, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int], year: int, color: str
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year, color)
