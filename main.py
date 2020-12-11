from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from flask import request
from google.cloud import datastore
from google.cloud import storage
import json 


def pull_test(request):

    json_data = get_json()
    project_id = "faasproject"
    subscription_id = "my-sub"
    timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    def callback(message):
        print(f"Received {message}.")
        complete = message.data
        print(f"Received comp {complete}.")
        result = json.loads(complete)
        json_data.update(result)
        print(f"Received res {result}.")
        message.ack()
        
        return f"Received {result}."
        

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
    create_json(json_data)
    print("Received {json_data}.")
    return f"Received"

def create_json(count):
    storage_client = storage.Client()
    json_file = count

    bucket_name = 'booksdatalist'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob('BooksData.json')
    blob.upload_from_string(
        data=json.dumps(json_file),
        content_type='application/json'
        )
    return 'UPLOAD COMPLETE'

def get_json():
    storage_client = storage.Client()
    bucket_name = 'booksdatalist'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob('BooksData.json')
    fileData = json.loads(blob.download_as_string())
    return fileData

