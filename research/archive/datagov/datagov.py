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

connect('falsepositv')

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

with open('names_translation.json') as f:
    names_translation = json.load(f)

ids_to_names = {k: names_translation.get(v.strip(),v) for k, v in ids_to_heb_names.items()}

ids_to_names

# + jupyter={"outputs_hidden": true} tags=[]
datasets_search_response['result']['results'][-1]


# -

def read_frame(resource_id, batch_size):
    recs = requests.get(
        f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource["id"]}&limit={batch_size}').json()        
    return pd.DataFrame(recs['result']['records'])


# + jupyter={"outputs_hidden": true}

# -

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

import pdb
pdb.pm()

# +
# total_read_bytes/total_read_rows = bytes per row 

# + tags=[]
upload_frames(datasets_search_response, batch_size=1000000, cap=None, upload=False)

# + jupyter={"outputs_hidden": true} tags=[]



# + tags=[]
from json import dumps, dump
re.findall('.{100}dcf999.{100}', dumps(j))
dump(j, open('d.json', 'w'), indent='   ')
# -

'covid-19' in j['result']

247871472

from itertools import chain
resource_ids=list(chain(*non_flat_resource_ids))



js = []
for resource_id in resources:
    js.append(requests.get(f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}").json()

# %load_ext autoreload
# %autoreload 2

connect('falsepositv')

headers = {'User-Agent': 'datagov-external-client'}
main_resp = requests.get(f'{base_url}/dataset/covid-19', headers=headers)
main_resp.raise_for_status()
main_content = main_resp.content

'dcf999c1-d394-4b57-a5e0-9d014a62e046' in html

urls = re.findall(r'dataset/(.+)/resource/([^""/]+)/?',html)
'dcf999c1-d394-4b57-a5e0-9d014a62e046' in [x[1] for x in urls]

re.findall(r'.{10}a9588029-8dd6-4c6f-b4ff-e8ca6413642f.{10}', html)

requests.get(f'https://data.gov.il/api/3/action/datastore_search?dataset_id=943f40df-d2e0-481b-9f06-04386c8e2419&limit=10').json()

len(urls)/4

re.search('.{10}486c9814-381a-4b7b-be20-43208f3151e1.{10}', html)

import json

# ### Since

# + tags=[]
# q = {
#     'result_date': '2020-05'
# }
j2 = requests.get(
    f'https://data.gov.il/api/3/action/datastore_search?resource_id=a9588029-8dd6-4c6f-b4ff-e8ca6413642f&limit=20&offset=3141592').json()

# + tags=[]
fr = pd.DataFrame(j2['result']['records'])
fr.result_date = pd.to_datetime(fr.result_date)
# -

fr

from ezchart import bokeh_xy

bokeh_xy.Dots('result_date', '_id', fr).datetime()

# + jupyter={"outputs_hidden": true} tags=[]
search_response
# -

# #### read data

trans

# +

[trans.get(x.strip()) for x in names.values()]
# -

for key, heb_name in names.items():
    eng_name = trans[heb_name.strip()]
    params = {'field_delimiter': ',', 
              'skip_leading_rows': '1', 
              'write_disposition': 'APPEND', 
              'file_format': 'CSV', 
              'data_path_template': f'gs://falsepositiv-datagov/{eng_name}/*/{eng_name}_*.csv', 
              'destination_table_name_template': eng_name, 
              'max_bad_records': '0'}
    cmd = f'''
bq mk \
    --transfer_config \\
    --project_id=falsepositv \\
    --data_source=google_cloud_storage \\
    --display_name={eng_name} \\
    --target_dataset=datagov \\
    --params='{json.dumps(params)}'
    '''
    print(cmd)

from datetime import datetime

NotFound

bucket.blob(f't/_id.txt').download_as_string()




# + jupyter={"outputs_hidden": true}
search_response

# + tags=[]
names_translation
# -

frames = read_frames(search_response, True, 1, 1)

pd.to_numeric(frames['isolations'].isolated_today_contact_with_confirmed)

normalise_and_schema_detect(frames['isolations'])[1]

bio = io.BytesIO()
frames['lab2020'].head(1).to_csv(bio, encoding='utf-8')
b = bio.getvalue()

# + tags=[]
b.decode()
# -

len(names)

frames.keys()

heb[-1] = heb[-1][:-1]

len(names)-len(heb)

trans = dict(list(zip(heb, names)))



# + jupyter={"outputs_hidden": true} tags=[]
s[0]
# -

resource_ids = set(re.findall('resource/([0-9a-z-]+)/', html))

# #### BQ load

from google.cloud.bigquery import Client as BQClient

from bqutils import *

from google.cloud.bigquery import SchemaField
bq_client = BQClient()

from bqutils import *

frames

frames.keys()

bq_client = bigquery.Client.from_service_account_json(json_credentials_path='/home/jovyan/notebooks/falsepositv/falsepositv.json')

#fr = worksheet_to_frame(workbooks[2][1].worksheets[1]).iloc[12:]
for name, frame in frames.items():
    print(name)
    normalised_frame, schema = normalise_and_schema_detect(frame) 
    print(schema)
    upload_to_bq(bq_client, normalised_frame.head(0), f'falsepositv.datagov.{name}', schema=schema)

frames[names[6]]

# +
QUERY = (
    'SELECT 1'
)
query_job = bq_client.query(QUERY)
rows = query_job.result()

for row in rows:
    print(row)
# -

# ### Scan buckets

blobs = bucket.list_blobs(prefix='lab2020')

l = list(blobs)

bl = l[0]





bl.name

# + jupyter={"outputs_hidden": true} tags=[]
re.findall('2020-(\d\d)', s.decode())
# -

r = []
for blob in l:
    s = blob.download_as_string()
    months = re.findall('2020-(\d\d)', s.decode())
    for month in months:
        r.append((bl.name, month))

len(r)

fr = pd.DataFrame(r, columns=['file', 'month'])

fr.groupby('month').size()

# ### rearange files

l = list(bucket.list_blobs())

get_name_regex = re.compile(r'([a-zA-Z0-9_]+)_(\d+)\.csv')

new_names = [(b,f'{get_name_regex.match(b.name).group(1)}/2021-08-14/{b.name}') for b in l if get_name_regex.match(b.name)]

# + jupyter={"outputs_hidden": true} tags=[]
for k, v in new_names:
    try:
        bucket.rename_blob(
            k, v
        )
        print(k,v)
    except:
        pass

# + tags=[]
from collections import defaultdict
nums = defaultdict(lambda : 0)
for bl in l:
    n = bl.name
    try:
        nn = get_name_regex.findall(n)[0][0]
        
        suffix = int(get_name_regex.findall(n)[0][1])
        nums[nn] = max(nums[nn], suffix)
    except:
        pass
# -

dict(nums)

for k, v in nums.items():
    id_blob = bucket.blob(f'{k}/2021-08-14/{k}_{v}.csv')
    bio = io.BytesIO()
    try:
        bio.write(id_blob.download_as_string())
        bio.seek(0)
        fr= pd.read_csv(bio)
        _id = fr._id.values[-1]
        print(k, _id)
        bucket.blob(f'{k}/_id.txt').upload_from_string(str(_id))
    except Exception as e:
        print(e)


