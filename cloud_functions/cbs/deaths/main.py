from datetime import datetime
from google.cloud import storage
from google.api_core.exceptions import NotFound
import base64
import os
import json
from common import *
import io
import pandas as pd

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

columns = {'deaths_1': ['total',
  'total_male',
  'total_female',
  'total_jewish',
  'total_jewish_male',
  'total_jewish_female',
  'total_arab',
  'total_arab_male',
  'total_arab_female',
  'age_0_19',
  'age_20_24',
  'age_25_29',
  'age_30_34',
  'age_35_39',
  'age_40_44',
  'age_45_49',
  'age_50_54',
  'age_55_59',
  'age_60_64',
  'age_65_69',
  'age_70_74',
  'age_75_79',
  'age_80_84',
  'age_85_89',
  'age_90_plus'],
 'deaths_2': ['Nafa_Jerusalem',
  'Nafa_Zefat',
  'Nafa_Kinneret',
  'Nafa_Yizre_el',
  'Nafa_Akko',
  'Nafa_Golan',
  'Nafa_Haifa',
  'Nafa_Hadera',
  'Nafa_HaSharon',
  'Nafa_Petah_Tiqwa',
  'Nafa_Ramla_',
  'Nafa_Rehovot',
  'Nafa_Tel_Aviv',
  'Nafa_Ashqelon',
  'Nafa_Be_er_Sheva',
  'Nafa_Judea_and_Samaria_Area'],
 'deaths_3': ['Total_Jerusalem_District',
  'Total_Northern_District',
  'Total_Haifa_District',
  'Total_Central_District',
  'Total_Tel_Aviv_District',
  'Total_Southern_Distric',
  'Total_Judea_and_Samaria_Area',
  'Jews_and_Others_Jerusalem_District',
  'Jews_and_Others_Northern_District',
  'Jews_and_Others_Haifa_District',
  'Jews_and_Others_Central_District',
  'Jews_and_Others_Tel_Aviv_District',
  'Jews_and_Others_Southern_Distric',
  'Jews_and_Others_Judea_and_Samaria_Area',
  'Arabs_Jerusalem_District',
  'Arabs_Northern_District',
  'Arabs_Haifa_District',
  'Arabs_Central_District',
  'Arabs_Southern_District']}
def read_frame_from_blob(blob):
    bio = io.BytesIO()
    bio.write(blob.download_as_string())
    bio.seek(0)
    return pd.read_csv(bio)    
def upload_frames(event, context):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT = '%Y-%m-%d'
    if 'data' in event:
        json_data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    else:
        print('No data in event, quitting')
        return  
    j = json_data['report_number']
    print(f'Running death reports {j}!')
    print(f"Google runtime: {os.environ.get('GOOGLE_RUNTIME')}")
    name = f'deaths_report_{j}'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('coviddatail')
    books = {}
    try:
        cbs_blobs = list(bucket.list_blobs(prefix=f'cbs/{name}'))
        sorted_cbs_blobs = sorted(list([x for x in cbs_blobs if '_id' not in x.name]), key=lambda x :x.updated)
        most_recent_frame = read_frame_from_blob(sorted_cbs_blobs[-1])
        most_recent_id_as_date = datetime.strptime(
        most_recent_frame.day.values[-1], DATE_FORMAT)

    except NotFound:
        most_recent_id_as_date = datetime(2000,1,1)
    print(fr'Loading only records after {most_recent_id_as_date}')
    # print("""This Function was triggered by messageId {} published at {} to {}
    # """.format(context.event_id, context.timestamp, context.resource["name"]))
    book = load_workbook_from_url(
        f'https://www.cbs.gov.il/he/publications/LochutTlushim/2020/p-{j}.xlsx')
    fr = worksheets_to_unified_frame(book, columns=['day'] + columns[f'deaths_{j}'],
        min_year=most_recent_id_as_date.year,
        debug=False)
    fr = fr.dropna(how='any').copy()
    fr['day'] = pd.to_datetime(fr['day'])

    frame = fr.loc[fr.day > most_recent_id_as_date, :]
    if len(frame) == 0:
        print(f"Skipping {name}, no records")        
        return
    day_from = frame.day.dt.date.values[0]
    day_to = frame.day.dt.date.values[-1]
    day_now = datetime.strftime(datetime.now(), '%Y-%m-%d')
    
    blob_name = f'cbs/{name}/{day_now}/{day_from}_{day_to}.csv' 
    bio = io.BytesIO()
    frame.to_csv(bio, index=False)
    blob = bucket.blob(blob_name)
    print(f"Uploading {len(frame)} records to blob {blob_name}")
    blob.upload_from_string(bio.getvalue()) 
    #_new_id = datetime.strftime(frame['day'].max(), TIME_FORMAT)
    # _id_blob = bucket.blob(f'cbs/{name}/_id.txt')
    # _id_blob.upload_from_string(_new_id)

def main():
    upload_frames({'data': 
        base64.b64encode(json.dumps({'report_number': 2}).encode('utf-8'))
        }, None)
    
if __name__ == '__main__':
    main()    