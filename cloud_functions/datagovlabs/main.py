from datetime import datetime
from google.cloud import storage
import base64
from common import upload_one_batch


IDS_TO_NAMES = {
     'd337959a-020a-4ed3-84f7-fca182292308': 'tested_individuals_features',
     'bf65a826-2440-43a3-b6a6-92bb45fe061f': 'lab2021a',
     'dcf999c1-d394-4b57-a5e0-9d014a62e046': 'lab2021b',
     'a9588029-8dd6-4c6f-b4ff-e8ca6413642f': 'lab2020',
}
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
                batch_size=100000, 
                cap=None, 
                upload=True)
            done = done and not resource_done
            print(done)


    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """
    try:
        print("""This Function was triggered by messageId {} published at {} to {}
        """.format(context.event_id, context.timestamp, context.resource["name"]))
    except:
        pass

def main():
    upload_frames(None, None)
    
if __name__ == '__main__':
    main()    

# ### Read data as frames
# def upload_frame(bucket, resource_id, name, upload=True, max_per_frame=-1, batch_size=100, cap=10000, once=False, start_id=None):
#     _id_blob = bucket.blob(f'{name}/_id.txt')
#     try:
#         _id = int(_id_blob.download_as_string().decode())
#         print(id)
#     except Exception as e:
#         print(e)
#         _id = 0
#     if start_id is not None:
#         _id = start_id
#     offset = _id
#     total_read_bytes = 0
#     total_read_rows = 0            
#     while True:
#         blob_name = f'{name}/{date}/{name}_{offset}_{offset + batch_size}.csv'

#         print(f'Reading {name} from offset {offset}')
#         recs = requests.get(
#             f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit={batch_size}&offset={offset}').json()        
#         if 'result' not in recs:
#             print(recs)
#             return once
#         if len(recs['result']['records']) == 0:
#             print(f'Done with {name}')
#             return once
#         offset = offset + batch_size
#         _id = recs['result']['records'][-1]['_id']
#         if upload:
#             _id_blob.upload_from_string(str(_id))
#         fr = pd.DataFrame(recs['result']['records'])
#         for c in fr.columns:
#             if 'date' in c:
#                 print('Read until: ', fr[c].values[-1])
#         bio = io.BytesIO()
#         fr.to_csv(bio)
#         v = bio.getvalue()
#         total_read_bytes = total_read_bytes + len(v)
#         total_read_rows = total_read_rows + len(recs['result']['records'])
#         blob = bucket.blob(blob_name)
#         if upload:
#             print(f"Uploading {len(recs['result']['records'])} records to blob {blob_name} records of id {offset}")
#             blob.upload_from_string(bio.getvalue()) 
#         if len(recs['result']['records']) < batch_size or once:
#             print(f'Done with {name}')
#             return once
