# function: genaifun1

import logging
import urllib.request
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import datetime
import pandas as pd
import openai
import os


connection_string = 'DefaultEndpointsProtocol=https;AccountName=genaiazurefun;AccountKey=m8WBdyeSy8tctCi/4phepBAcQhy0VhtiN+3nWsl0/w+F00HesbMGb8bz6KuS073l4kS3S6Wif4+L+AStyeZ+Qg==;EndpointSuffix=core.windows.net'
input_container_name = 'container1'
output_container_name = 'container1output'

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(input_container_name)

def main(myblob: func.InputStream):
    # Get API key
    openai_api_key = os.environ['OPENAI_API_KEY']



    ### INPUT(S)
    # Basic logging
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    # The value of myblob.name is of the pattern: <container_name/file_name>
    blob_url = f"https://genaiazurefun.blob.core.windows.net/{myblob.name}"

    # Use urllib to request the file, using blob_url
    with urllib.request.urlopen(blob_url) as response:
        context = response.read().decode('utf-8')


    ### TRANSFORMATION(S)
    # Split the lines of the context variable into separate rows
    lines = context.split('\n')
    rows = [line.split(',') for line in lines]

    # Remove leading and trailing whitespace from column names
    column_names = [name.strip() for name in rows[0]]

    # Create a pandas DataFrame from the rows
    df = pd.DataFrame(rows[1:], columns=column_names)
    logging.info(f"\n===============\nDataframe Contents:\n{df}\n===============\n")

    '''
    # Do a small upper transform on the review colum and log it
    df['review'] = df['review'].str.upper()
    reviews = df['review']
    logging.info(f"\n===============\nReviews:\n{reviews}\n===============\n")
    '''


    ### OPENAI
    # Convert the dataframe back to a .csv for passing to OpenAI within a prompt
    csv_data = df.to_csv(index=False)

    openai.api_key = openai_api_key
    response = openai.ChatCompletion.create(
        model = 'gpt-3.5-turbo-16k',
        messages = [
            {'role': 'system','content':f"I am going to give you csv data.Output csv data here with [product, review, type] headers where you use semantic analysis to determine if the review was [good, bad or neutral]. Here is the data: {csv_data}"}]
    )
    content = response.choices[0].message['content'].strip()



    ### OUTPUT
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    file_name=f"output_{timestamp}.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(output_container_name)
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(content, overwrite=True)