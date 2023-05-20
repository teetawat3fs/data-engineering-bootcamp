import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"

# keyfile = os.environ.get("KEYFILE_PATH")

# keyfile_bigquery_gsc
keyfile_gcs = "lateral-boulder-384606-7a443bca58fe-bigquery-gcs.json"
service_account_info = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info)
# keyfile_bigquery_gsc
keyfile_bigquery = "lateral-boulder-384606-7a443bca58fe-bigquery-gcs.json"
service_account_info = json.load(open(keyfile_bigquery))
credentials_bigquery = service_account.Credentials.from_service_account_info(service_account_info)

project_id = "lateral-boulder-384606"
bucket_name = "deb-bootcamp-100016"

def upload_to_gcs_to_bigquery(data, dt=None):
    # setting variable
    project_id = "lateral-boulder-384606"
    bucket_name = "deb-bootcamp-100016"
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)
    # with datetime
    if dt:
        # load data from local to GCS
        file_path = f"{DATA_FOLDER}/{data}.csv"
        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

        # load data from GCS to Bigquery
        bigquery_client = bigquery.Client(
            project=project_id,
            credentials=credentials_bigquery,
            location=location,
        )
        partition = dt.replace("-", "")
        table_id = f"{project_id}.deb_practice.{data}${partition}"
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
            ),
        )
        job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{destination_blob_name}",
            table_id,
            job_config=job_config,
            location=location,
        )
        job.result()

        table = bigquery_client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    #without datetime
    else:
        # load data from local to GCS
        file_path = f"{DATA_FOLDER}/{data}.csv"
        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)

        # load data from GCS to Bigquery
        bigquery_client = bigquery.Client(
            project = project_id,
            credentials = credentials_bigquery,
            location = location,
        )
        table_id = f"{project_id}.deb_practice.{data}"
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            )
        job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{destination_blob_name}",
            table_id,
            job_config=job_config,
            location=location,
        )
        job.result()

        table = bigquery_client.get_table(table_id)
        print(f"loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

upload_to_gcs_to_bigquery("addresses")
upload_to_gcs_to_bigquery("events", "2021-02-10")
upload_to_gcs_to_bigquery("order_items")
upload_to_gcs_to_bigquery("orders", "2021-02-10")
upload_to_gcs_to_bigquery("products")
upload_to_gcs_to_bigquery("promos")
upload_to_gcs_to_bigquery("users", "2021-10-23")
