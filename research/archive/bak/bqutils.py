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
import re
import pandas as pd


# +
from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig
from google.cloud.bigquery import SchemaField

def normlise_column(c):
    c1 = re.sub(r'[^0-9A-Za-z]', '_', c)
    return c1
def connect(service_account):
    from glob import glob
    files = glob(f'/home/jovyan/notebooks/.google/{service_account}*')
    if len(files) != 1:
        raise Exception(f'Found {len(files)} files')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = files[0]
    return bigquery.Client()
def normalise_and_schema_detect(fr):
    schema = []
    output_fr = fr.copy()
    output_fr.columns = [normlise_column(c) for c in output_fr.columns]
    for c in output_fr.columns:
        try: 
            if 'date' in c.lower():
                output_fr[c] = pd.to_datetime(output_fr[c])
                schema.append(SchemaField(c, "DATETIME", "NULLABLE"))
            else:
                output_fr[c] = pd.to_numeric(output_fr[c])
                try:
                    output_fr[c] = output_fr[c].astype(int)
                    schema.append(SchemaField(c, "INTEGER", "NULLABLE"))
                except:
                    schema.append(SchemaField(c, "FLOAT", "NULLABLE"))                    
        except Exception as e:
            print(e)
            schema.append(SchemaField(c, "STRING", "NULLABLE"))
            
    return (output_fr, schema)

# Construct a BigQuery client object.
def upload_to_bq(client, dataframe, table_id, schema=None):
    job_config = LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        skip_leading_rows=1, 
        autodetect=schema is None,
        write_disposition="WRITE_TRUNCATE",
    )

    if schema: 
        job_config.schema = schema
    job = client.load_table_from_dataframe(dataframe.reset_index(),                                           
                                      table_id,                                           
                                      job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
