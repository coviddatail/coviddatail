    # ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# ### Imports

# %load_ext autoreload
# %autoreload 2

import pandas as pd
import io
from datetime import datetime
from google.cloud import storage
import base64
import requests
import re
from urllib import parse
from json import dumps

# -

# ### Read data as frames
def upload_one_batch(bucket, resource_id, name, upload=True, max_per_frame=-1, batch_size=100, cap=10000,  start_id=None):
    print(name)
    _id_blob = bucket.blob(f'datagov/{name}/_id.txt')
    print(_id_blob)
    try:
        _id = int(_id_blob.download_as_string().decode())
        print(id)
    except Exception as e:
        print(e)
        _id = 0
    if start_id is not None:
        _id = start_id
    offset = _id
    date = datetime.strftime(datetime.now(), '%y-%m-%d')

    blob_name = f'datagov/{name}/{date}/{name}_{offset}_{offset + batch_size}.csv'    
    print(f'Reading {name} from offset {offset}')
    recs = requests.get(
        f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit={batch_size}&offset={offset}').json()        
    if 'result' not in recs:
        print(recs)
        return False
    if len(recs['result']['records']) == 0:
        print(f'Done with {name}')
        return False
    offset = offset + batch_size
    _id = recs['result']['records'][-1]['_id']
    if upload:
        _id_blob.upload_from_string(str(_id))
    fr = pd.DataFrame(recs['result']['records'])
    for c in fr.columns:
        if 'date' in c:
            print('Read until: ', fr[c].values[-1])
    bio = io.BytesIO()
    fr.to_csv(bio, index=False)
    v = bio.getvalue()
    blob = bucket.blob(blob_name)
    if upload:
        print(f"Uploading {len(recs['result']['records'])} records to blob {blob_name} records of id {offset}")
        blob.upload_from_string(bio.getvalue()) 
    if len(recs['result']['records']) < batch_size:
        print(f'Done with {name}')
        return False
    return True
