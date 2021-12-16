import io
import requests
import pandas as pd
from datetime import datetime
batch_size = 100000
from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.get_bucket('falsepositiv-datagov')
trans = {'נתוני קורונה בידודים': 'isolations',
 'נתוני מעבדה 2020 - Covid-19 Y2020': 'lab2020',
 'נתוני קורונה בדיקות מעבדה 2021 COVID-19 lab data': 'lab2021',
 'נתוני קורונה מאפייני נבדקים tested individuals': 'tested_individuals_features',
 'נתוני קורונה מאפייני נבדקים - שבועיים אחרונים בלבד': 'tested_individuals_features_last2w',
 'נתוני קורונה מאפייני נבדקים - טבלת עזר': 'tested_individuals_features_helper',
 'נתוני קורונה איזורים סטטיסטיים covid-19 by area': 'covid19_by_area',
 'נתוני קורונה קבוצות מין וגיל': 'covid19_by_age',
 'תחלואת קורונה בקרב צוות רפואי בבתי חולים': 'covid19_infection_hospital_crew',
 'נתוני קורונה נפטרים': 'covid19_deaths',
 'טבלת ישובים': 'covid19_communities',
 'גילאי המתחסנים': 'vaccinated_age_groups',
  'תמותה ואשפוזים הקשורים לקורונה לאחר החיסון': 'deaths_related_to_vaccine',
 'מתחסנים על פי ישוב': 'vaccinated_communities',
 'אימותים לאחר החיסון': 'positives_after_vaccination'
         }
search_response = requests.get('https://data.gov.il/api/3/action/package_search?q=covid', headers = {'User-Agent': 'datagov-external-client'}).json()
blob_names = [x.name for x in bucket.list_blobs()]
last_read_id = 
for p, result in enumerate(search_response['result']['results']):
    for j, resource in enumerate(result['resources']):
        if resource['format'] != 'CSV':
            continue
        print(resource['size'])
        name = trans.get(resource['name'].strip(), resource["name"].strip())
        k = 0
        r = 0
        while True:
            blob_name = f'{name}_{k}.csv'
            if blob_name in blob_names:
                k = k +1
                print('skipping', blob_name)
                continue
            recs = requests.get(f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource["id"]}&limit={batch_size}&offset={batch_size*k}').json()        
            k = k + 1
            if len(recs['result']['records']) < batch_size:
                break
            print(blob_name, datetime.now(), len(recs['result']['records']), batch_size*k)
            blob = bucket.blob(blob_name)
            k = k + 1
            r = r + len(recs['result']['records'])
            print(r)            
            fr = pd.DataFrame(recs['result']['records'])
            bio = io.BytesIO()
            fr.to_csv(bio)
            blob.upload_from_string(bio.getvalue())            