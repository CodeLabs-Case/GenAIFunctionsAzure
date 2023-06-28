# function: genaifun1

import logging
import urllib.request
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import datetime

connection_string = 'DefaultEndpointsProtocol=https;AccountName=genaiazurefun;AccountKey=m8WBdyeSy8tctCi/4phepBAcQhy0VhtiN+3nWsl0/w+F00HesbMGb8bz6KuS073l4kS3S6Wif4+L+AStyeZ+Qg==;EndpointSuffix=core.windows.net'
input_container_name = 'container1'
output_container_name = 'container1output'

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(input_container_name)

def main(myblob: func.InputStream):

    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    

    # with urllib.request.urlopen(f"https://genaiazurefun.blob.core.windows.net/container1/{myblob.name}") as response:
    #     context = response.read().decode('utf-8')

    blob_url = [
        f"https://genaiazurefun.blob.core.windows.net/{myblob.name}",
        "https://genaiazurefun.blob.core.windows.net/container1/data202306271438.csv"
    ]

    with urllib.request.urlopen(blob_url[0]) as response:
        context = response.read().decode('utf-8')
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    file_name=f"output_{timestamp}.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(output_container_name)
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(context, overwrite=True)