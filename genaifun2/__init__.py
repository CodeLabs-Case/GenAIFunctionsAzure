# function: genaifun2

import logging
import urllib.request
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import datetime
import pandas as pd
import openai
import os
import re
import time

connection_string = 'DefaultEndpointsProtocol=https;AccountName=genaiazurefun;AccountKey=m8WBdyeSy8tctCi/4phepBAcQhy0VhtiN+3nWsl0/w+F00HesbMGb8bz6KuS073l4kS3S6Wif4+L+AStyeZ+Qg==;EndpointSuffix=core.windows.net'
input_container_name = 'container2'
output_container_name = 'container2output'

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(input_container_name)

def f_outline(prompt):
    response = openai.ChatCompletion.create(
        model = 'gpt-3.5-turbo-16k',
        messages = [
            {'role': 'system','content':f"{prompt}"}]
    )
    outline = response.choices[0].message['content'].strip()
    logging.info(f"\n\nOutline: {outline}\n\n")

    return outline

def f_expansion(outline):
    response = openai.ChatCompletion.create(
        model = 'gpt-3.5-turbo-16k',
        messages = [
            {'role': 'system','content':f"{outline}.\n How many numbered points are in the above outline, do not include sub-points (letters)?"}]
    )
    n_sentence = response.choices[0].message['content'].strip()

    # Get 'n' from the sentence
    number = re.findall(r'\d+', n_sentence)
    n = int(number[0])
    logging.info(f"There are {n} points in the outline")

    # Loop over each point in the outline, expand it, append the results to the string, then output to the final file
    final_string = ""
    for i in range(n):
        i = i + 1
        response = openai.ChatCompletion.create(
        model = 'gpt-3.5-turbo-16k',
        messages = [
            {'role': 'system','content':f"{outline}.\n Expand on point {i}, don't include your own response like 'certainly.'"}]
        )
        final_string += "\n" + response.choices[0].message['content'].strip()
    
    return final_string

def f_facts(outline):
    response = openai.ChatCompletion.create(
        model = 'gpt-3.5-turbo-16k',
        messages = [
            {'role': 'system','content':f"Context: {outline}\nGive me a list of facts stated in the above context, don't include your own response like 'certainly."}]
    )
    facts = response.choices[0].message['content'].strip()
    logging.info(f"\n\nFacts: {facts}\n\n")
    
    return facts

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
        prompt = response.read().decode('utf-8')



    ### OPENAI
    openai.api_key = openai_api_key
    
    # Outline: Get the outline, re-use it 'n' times for each point
    outline = f_outline(prompt)
    
    # Expansion: Get the number of points in the outline 'n'. Commented out due to rate limit issues.
    #expansion = f_expansion(outline)

    # Facts: Get the facts in the generated content
    facts = f_facts(outline)



    ### OUTPUT
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    file_name=f"output_{timestamp}.txt"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(output_container_name)
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(facts, overwrite=True)
