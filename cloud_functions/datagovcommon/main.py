from datetime import datetime, timedelta
from google.cloud import storage
import base64
from common import upload_one_batch
import json

IDS_TO_NAMES = {
     'd337959a-020a-4ed3-84f7-fca182292308': 'tested_individuals_features',
     'bf65a826-2440-43a3-b6a6-92bb45fe061f': 'lab2021a',
     'dcf999c1-d394-4b57-a5e0-9d014a62e046': 'lab2021b',
     'a9588029-8dd6-4c6f-b4ff-e8ca6413642f': 'lab2020',
     '8dce17a5-70f5-4653-876f-32f81c1a9d3c': 'young_population_table',
     '9eedd26c-019b-433a-b28b-efcc98de378d': 'isolations',
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
NAMES_TO_IDS = {v: k for k, v in IDS_TO_NAMES.items()}
storage_client = storage.Client()
bucket = storage_client.get_bucket('coviddatail')



def upload_frames(event, context):
    start = datetime.now()
    if 'data' in event:
        json_data = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    name = json_data['dataset_name']
    resource_id = json_data.get('resource_id',
        NAMES_TO_IDS[name])
    batch_size = json_data.get('batch_size', 10000)
    done = False
    while True:
        if datetime.now() - start > timedelta(seconds=300):
            print('Applicative timeout')
            break
        if not upload_one_batch(bucket, 
                resource_id, name, 
                batch_size=batch_size, 
                cap=None, 
                upload=True):
            break

def main():
    upload_frames({'data': 
        base64.b64encode(json.dumps({'dataset_name': 'lab2021a', 
                                     'batch_size':100000}
            ).encode('utf-8'))
        }, None)
    
if __name__ == '__main__':
    main()    
