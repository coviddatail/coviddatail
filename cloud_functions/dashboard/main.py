import requests
import re
import io
import pandas as pd
import json
from datetime import datetime
from google.cloud import storage
import base64
dashboard_url = 'https://datadashboardapi.health.gov.il/api/queries/_batch'

dataset_names = ['averageInfectedPerWeek',
 'breatheByPeriodAndAgeAndGender',
 'deadByPeriodAndAgeAndGender',
 'deadPatientsPerDate',
 'infectedByPeriodAndAgeAndGender',
 'infectedPerDate',
 'infectionFactor',
 'patientsPerDate',
 'researchGraph',
 'severeByPeriodAndAgeAndGender',
 'spotlightAggregatedPublic',
 'spotlightPublic',
 'testResultsPerDate',
 'testsPerDate',
 'vaccinated',
 'vaccinationsPerAge']



headers={'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
     'Accept': 'application/json, text/plain, */*',
     'sec-ch-ua-mobile': '?0',
     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
     'Content-Type': 'application/json'}



def fr_with_prefix(prefix, fr):
    fr2 = fr.copy()
    fr2.columns = [f'{prefix}_{c}' for c in fr.columns]
    return fr2
        
def parse_data(data):
    if len(data) == 1:
        return pd.DataFrame([[]])  
    fr = pd.DataFrame(data)
    types = fr.apply(lambda row: row.apply(type), axis=0).drop_duplicates().iloc[0, :]
    
    while True:
        simple_columns = [cname for cname in fr.columns if types[cname] not in (dict, list) ]
        complex_columns = [cname for cname in fr.columns if types[cname]  in  (dict, list)]
        if len(complex_columns) == 0:            
            break
        print(len(complex_columns))
        no_dicts = fr.loc[:, simple_columns]
        print(simple_columns, complex_columns)
        fr = pd.concat([no_dicts] + [fr_with_prefix(c, fr[c].apply(pd.Series)) for c in complex_columns], axis=1)
        types = fr.apply(lambda row: row.apply(type), axis=0).drop_duplicates().iloc[0,:]
    return fr
            
storage_client = storage.Client()
bucket = storage_client.get_bucket('coviddatail')


def upload_batch(event, context):
    formatted_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
    reqs = []
    for id, dataset_name in enumerate(dataset_names):
        reqs.append({"id":id,
            "queryName":dataset_name,
            "single":False,
             "parameters":{'days':160}})
    resp = requests.post(dashboard_url, headers=headers, data=json.dumps({"requests":reqs}))
    resp.raise_for_status()
    datasets = resp.json()    
    frs = {dataset_names[j]: 
        parse_data(dataset['data']).assign(batch_date=formatted_date) for 
            j, dataset in enumerate(datasets)}
    for name, fr in frs.items():        
        blob_name = f'dashboard/{name}/{formatted_date}.csv' 
        bio = io.BytesIO()
        fr.to_csv(bio, index=False)
        blob = bucket.blob(blob_name)
        print(f"Uploading {len(fr)} records to blob {blob_name} records of batch {formatted_date}")
        blob.upload_from_string(bio.getvalue()) 



if __name__ == '__main__':
    upload_batch()