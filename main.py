from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from flask import request
from google.cloud import datastore
from google.cloud import storage
import json 

def scraped_data(request):
    storage_client = storage.Client()
    bucket_name = 'booksdatalist'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob('BooksData.json')
    fileData = json.loads(blob.download_as_string())
    print(test)
    return addCors(json.dumps(fileData))

def addCors(response, code=200):
    headers = {'Access-Control-Allow-Origin': '*'}
    return (response, code, headers)