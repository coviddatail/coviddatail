from datetime import datetime
from google.cloud import storage
import base64
from common import upload_one_batch


IDS_TO_NAMES = {
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
storage_client = storage.Client()
bucket = storage_client.get_bucket('coviddatail')





# Round robin 
def upload_frames(event, context):
    done = False
    while not done:
        done = True
        for resource_id, name in IDS_TO_NAMES.items():
            print(name)
            resource_done = upload_one_batch(bucket, 
                resource_id, name, 
                batch_size=10000, 
                cap=None, 
                upload=True)
            done = done and not resource_done
            print(done)
def main():
    upload_frames(None, None)
    
if __name__ == '__main__':
    main()    