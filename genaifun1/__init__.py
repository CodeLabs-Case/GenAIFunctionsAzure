# function: genaifun1

import logging
import urllib.request
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import datetime
import pandas as pd

connection_string = 'DefaultEndpointsProtocol=https;AccountName=genaiazurefun;AccountKey=m8WBdyeSy8tctCi/4phepBAcQhy0VhtiN+3nWsl0/w+F00HesbMGb8bz6KuS073l4kS3S6Wif4+L+AStyeZ+Qg==;EndpointSuffix=core.windows.net'
input_container_name = 'container1'
output_container_name = 'container1output'

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(input_container_name)

def main(myblob: func.InputStream):

    # Basic logging
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    # The value of myblob.name is of the pattern: <container_name/file_name>
    blob_url = f"https://genaiazurefun.blob.core.windows.net/{myblob.name}"

    # Use urllib to request the file, using blob_url
    with urllib.request.urlopen(blob_url) as response:
        context = response.read().decode('utf-8')

    # Split the lines of the context variable into separate rows
    lines = context.split('\n')
    rows = [line.split(',') for line in lines]

    # Create a pandas DataFrame from the rows
    df = pd.DataFrame(rows[1:], columns=rows[0])

    logging.info(f"\n===============\nDataframe Contents:\n{df}\n===============\n")
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    file_name=f"output_{timestamp}.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(output_container_name)
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(context, overwrite=True)