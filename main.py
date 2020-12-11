from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from flask import request
from google.cloud import datastore
from google.cloud import storage
import json 

# def push_to_dataStore():
#     datastore_client = datastore.Client()
#     key = datastore_client.key('users', '213')
#     task = datastore.Entity(key)
#     task.update({
#         'name': 'Bob',
#         'transaction_id': 1878,
#         'revenue': 135.55,
#         'paid': True
#     })
#     datastore_client.put(task)

#     docs = datastore_client.query(kind='users').fetch()
#     for doc in docs:
#         print(doc.key.name)
#         print(doc)
def scraped_data(request):
    storage_client = storage.Client()
    bucket_name = 'booksdatalist'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob('BooksData.json')
    fileData = json.loads(blob.download_as_string())
    return addCors(json.dumps(fileData))

def addCors(response, code=200):
    headers = {'Access-Control-Allow-Origin': '*'}
    return (response, code, headers)

def pull_test(request):

    json_data = get_json()
    # TODO(developer)
    project_id = "faasproject"
    # project_id = "your-project-id"
    subscription_id = "my-sub"
    # Number of seconds the subscriber should listen for messages
    timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
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

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
    # create_json()
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

