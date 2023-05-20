# Ref. https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import pandas as pd
import os 
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account

keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "lateral-boulder-384606"
client = bigquery.Client(
    project=project_id, 
    credentials=credentials,
)

filelist = [
    'data/addresses.csv',
    'data/events.csv',
    'data/order_items.csv',
    'data/orders.csv',
    'data/products.csv',
    'data/promos.csv',
    'data/users.csv'
]
# Create Function Upload
def upload(path: str):
    job_config = None
    table = path.split('/')[1].replace(".csv", "")
    if table in ['events', 'orders', 'users']:
        job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema = [
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        ],
        source_format = bigquery.SourceFormat.CSV,
        autodetect = True,
        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        ),
        )
    else:
        job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format = bigquery.SourceFormat.CSV,
        autodetect = True,
        )
    file_path = path
    df = pd.read_csv(file_path)
    df.info()

    table_id = f"{project_id}.deb_bootcamp.{table}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# Run function upload
for path in filelist:
    upload(path)


       
