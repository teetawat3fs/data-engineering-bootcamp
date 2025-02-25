# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import os
from datetime import datetime

import pandas as pd
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

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("user", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("order", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("product", bigquery.SqlTypeNames.STRING),
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
    clustering_fields=["event_type"],
)

file_path = "data/events.csv"
df = pd.read_csv(file_path, parse_dates=["created_at"])
df.info()

table_id = f"{project_id}.deb_bootcamp.events"
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")