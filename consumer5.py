
from kafka import KafkaConsumer,TopicPartition
import json
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery    
import pandas as pd
import json
import re
import io
import datetime
import sys
import os
from io import StringIO, BytesIO

#variables

mydataset = "myds1"
bucket_name= "mykafkabuck"
dest_fold = "myfold"
myproject = "nihaproject1"
storage_client = storage.Client.from_service_account_json('cred.json')

bucket = storage_client.get_bucket(bucket_name)
client = bigquery.Client.from_service_account_json('cred.json')

def myconsumer():

    consumer = KafkaConsumer(
    client_id = "client1",
    group_id = None,
    bootstrap_servers =[<localhost:port>],
    auto_offset_reset='earliest',

    consumer_timeout_ms=10000,  
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    )

    print(consumer.topics())
    tp = TopicPartition('niharikatopic2', 0)
    consumer.assign([tp])
    consumer.seek_to_beginning()
    consumer.seek(tp, 10000)

    # records = consumer.poll(timeout_ms=10000)

    with open('Reply.json', 'a') as f:
        for r in consumer:
            f.write(str(r[6]))
            f.write('\n')
    print("data file downloaded at consumer side")

    
def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""
     
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        'cred.json')

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    
    print("data uploaded to gcs")
 


#Function to create a dataset in Bigquery
def bq_create_dataset(bigquery_client, dataset):
    dataset_ref = bigquery_client.dataset(dataset)
    try:
        dataset = bigquery_client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset))
    except :
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    print("dataset created")



def gcs_to_bq():    
    for blob in bucket.list_blobs(prefix=dest_fold):
        
        if blob.name.endswith('json'): #Checking for psv blobs as list_blobs also returns folder_name
            filename = re.findall(r".*/(.*).json",blob.name) #Extracting file name for BQ's table id
            filenametest=filename[0].strip().replace(" ", "")
            print(filenametest)

            table_id = f'{myproject}.{mydataset}.{filenametest}' # Determining table name
            print(table_id)
            temp=blob.name
            uri1= "gs://{}".format(bucket_name)
            uri =f'{uri1}/{temp}'
            print(uri)

            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            
            load_job = client.load_table_from_uri(
                uri,
                table_id,
                location="US",  # Must match the destination dataset location.
                job_config=job_config,
            )  # Make an API request.
            load_job.result()
            destination_table = client.get_table(table_id)
            print("Loaded {} rows.".format(destination_table.num_rows))
    print('data successfully loaded from gcs to bigquery')  



myconsumer()
upload_to_bucket("myfold/Reply.json", "Reply.json","mykafkabuck")
bq_create_dataset(client,mydataset)
gcs_to_bq()
    

