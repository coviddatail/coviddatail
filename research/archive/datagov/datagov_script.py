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
from fputils.bqutils import *

#connect('falsepositv')

import requests
import re
from urllib import parse
from json import dumps

# ### Search datasets

datasets_search_response = requests.get('https://data.gov.il/api/3/action/package_search?q=name:covid-19', headers = {'User-Agent': 'datagov-external-client'}).json()

# ### Read raw data

# + tags=[]
ids_to_heb_names = {}
resources = {}
for result in datasets_search_response['result']['results']:
    for resource in result['resources']:
        if resource['format'] != 'CSV':
            continue
            
        ids_to_heb_names[resource['id']] = resource['name']
#        j = requests.get(f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource["id"]}&limit=10').json()
        resources[resource['id']] = resource
# -

# ### Read data as frames

ids_to_names = {'a9588029-8dd6-4c6f-b4ff-e8ca6413642f': 'lab2020',
 'bf65a826-2440-43a3-b6a6-92bb45fe061f': 'lab2021a',
 '8dce17a5-70f5-4653-876f-32f81c1a9d3c': 'young_population_table',
 '9eedd26c-019b-433a-b28b-efcc98de378d': 'isolations',
 'dcf999c1-d394-4b57-a5e0-9d014a62e046': 'lab2021b',
 'd337959a-020a-4ed3-84f7-fca182292308': 'tested_individuals_features',
 '74216e15-f740-4709-adb7-a6fb0955a048': 'tested_individuals_features_last2w',
 '09e66a69-ad5b-4c46-a5d9-1d1479b1f338': 'tested_individuals_features_helper',
 'd07c0771-01a8-43b2-96cc-c6154e7fa9bd': 'covid19_by_area',
 '89f61e3a-4866-4bbf-bcc1-9734e5fee58e': 'covid19_by_age',
 '6253ce68-5f31-4381-bb4e-557260d5c9fc': 'covid19_infection_hospital_crew',
 'a2b2fceb-3334-44eb-b7b5-9327a573ea2c': 'covid19_deaths',
 '8a21d39d-91e3-40db-aca1-f73f7ab1df69': 'covid19_communities',
 '57410611-936c-49a6-ac3c-838171055b1f': 'vaccinated_age_groups',
 '12c9045c-1bf4-478a-a9e1-1e876cc2e182': 'vaccinated_communities',
 '9b623a64-f7df-4d0c-9f57-09bd99a88880': 'positives_after_vaccination',
 '8a51c65b-f95a-4fb8-bd97-65f47109f41f': 'deaths_related_to_vaccine',
 '32150ead-89f2-461e-9cc3-f785e9e8608f': 'vaccinated_groups'}

# + jupyter={"outputs_hidden": true} tags=[]
datasets_search_response['result']['results'][-1]


# -

def read_frame(resource_id, batch_size):
    recs = requests.get(
        f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource["id"]}&limit={batch_size}').json()        
    return pd.DataFrame(recs['result']['records'])



blob_names = [x.name for x in bucket.list_blobs()]


[x for x in blob_names if 'lab2021a' in x][-7]

# ### Loop all datasets

# #### Define function

# +

storage_client = storage.Client()
bucket = storage_client.get_bucket('falsepositiv-datagov')
date = datetime.strftime(datetime.now(), '%y-%m-%d')
blob_names = [x.name for x in bucket.list_blobs()]
def upload_frame(resource, name, upload=True, max_per_frame=-1, batch_size=100, cap=10000, start_id=None):
        _id_blob = bucket.blob(f'{name}/_id.txt')
        try:
            _id = int(_id_blob.download_as_string().decode())
            print(id)
        except Exception as e:
            print(e)
            _id = 0
        if start_id is not None:
            _id = start_id
        offset = _id
        total_size_in_bytes = resource['size']
        total_read_bytes = 0
        total_read_rows = 0            
        while True:
            blob_name = f'{name}/{date}/{name}_{offset}_{offset + batch_size}.csv'
            if blob_name in blob_names:
                offset = offset + batch_size
                print('skipping', blob_name)
                continue
            print(f'Reading {name} from offset {offset}')
            recs = requests.get(
                f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource["id"]}&limit={batch_size}&offset={offset}').json()        
            if 'result' not in recs:
                print(recs)
                break
            if len(recs['result']['records']) == 0:
                print(f'Done with {name}')
                break
            offset = offset + batch_size
            _id = recs['result']['records'][-1]['_id']
            if upload:
                _id_blob.upload_from_string(str(_id))
            fr = pd.DataFrame(recs['result']['records'])
            for c in fr.columns:
                if 'date' in c:
                    print('Read until: ', fr[c].values[-1])
            bio = io.BytesIO()
            fr.to_csv(bio)
            v = bio.getvalue()
            total_read_bytes = total_read_bytes + len(v)
            total_read_rows = total_read_rows + len(recs['result']['records'])
            print(f'Projection of amount read: {100*(total_read_bytes/total_read_rows * offset/total_size_in_bytes):.2f}%')
            blob = bucket.blob(blob_name)
            if upload:
                print(f"Uploading {len(recs['result']['records'])} records to blob {blob_name} records of id {offset}")
                blob.upload_from_string(bio.getvalue()) 
            if len(recs['result']['records']) < batch_size:
                print(f'Done with {name}')
                break
# -

# #### Run loop 

for resource_id, name in ids_to_names.items():
    resource = resources[resource_id]
    upload_frame(resource, name, batch_size=100000, cap=None, upload=True)
