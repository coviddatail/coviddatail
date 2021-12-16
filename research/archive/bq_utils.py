# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import os
import json




# +
from google.cloud import https://drive.google.com/drive/folders/1AV8TfuFFX6O7hGikv-D_ItcDDrsLCPct?usp=sharing
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/odedbadt/.google/odedbadt-1ca99f4a15b4.json'

# Construct a BigQuery client object.
def upload_to_bq(dataframe, table_name):
    client = bigquery.Client()
    table_id = f"odedbadt.hammer.{table_name}"

    dataframe.to_csv('/tmp/tmp.csv')
    with open('/tmp/tmp.csv', "rb") as source_file:
        job = client.load_table_from_file(csv_data, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

