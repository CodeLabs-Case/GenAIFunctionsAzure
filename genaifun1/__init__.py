# function: genaifun1

import logging
import urllib.request
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import datetime

connection_string = 'DefaultEndpointsProtocol=https;AccountName=genaiazurefun;AccountKey=m8WBdyeSy8tctCi/4phepBAcQhy0VhtiN+3nWsl0/w+F00HesbMGb8bz6KuS073l4kS3S6Wif4+L+AStyeZ+Qg==;EndpointSuffix=core.windows.net'
container_name = 'container1'

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

def main(myblob: func.InputStream):

    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    

    with urllib.request.urlopen('https://genaiazurefun.blob.core.windows.net/container1/data202306271436.csv') as response:
        context = response.read().decode('utf-8')
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    file_name=f"output_{timestamp}.csv"

    with open(file_name, 'w') as output_file:
        output_file.write(context)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(file_name)
    with open(file_name, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
