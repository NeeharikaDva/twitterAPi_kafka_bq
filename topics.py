import requests
import os
import json
import pandas as pd
from time import sleep  
from json import dumps  
import time
# import tweepy
from datetime import datetime
from kafka import KafkaProducer 
from kafka import KafkaConsumer




def create_headers(bearer_token):
    headers = {
        "Authorization": "Bearer {}".format(bearer_token),
        "User-Agent": "v2SpacesSearchPython"
    }
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    bearer_token = ""
    search_url = "https://api.twitter.com/2/spaces/search"

    topics = ['nature','politics','god'] # Replace this value with your search term
    # topics = ['politics'] # Replace this value with your search term

    for topic in topics:
        
        # Optional params: host_ids,conversation_controls,created_at,creator_id,id,invited_user_ids,is_ticketed,lang,media_key,participants,scheduled_start,speaker_ids,started_at,state,title,updated_at
        query_params = {'query': topic, 'space.fields': 'title,created_at', 'expansions': 'creator_id'}
        count=100
        headers = create_headers(bearer_token)
        json_response = connect_to_endpoint(search_url, headers, query_params)
        file= json.dumps(json_response, indent=4, sort_keys=True)
        print("topic : "+ topic)
        print(file)
        # file.encode('utf-8')
        df = pd.DataFrame(json_response['data'])
        print(df)
        topic_name = "niharikatopic2"
        producer = KafkaProducer(bootstrap_servers='35.189.27.187:9092')
        # try:
        #     # producer.send(topic_name, str.encode(file))
        #     producer.send(topic_name,json.dumps(json_response, indent=4, sort_keys=True).encode('utf-8'))

        # except:
        #     pass
        # finally:
        #     producer.close()
        # print("done")
        # producer.send(topic_name,"hello")
        # producer.send(topic_name,str.encode("{["))
        for row in json_response['data']:
            producer.send(topic_name,json.dumps(row, indent=4, sort_keys=True).encode('utf-8'))
            # producer.send(topic_name,str.encode(','))
        # producer.send(topic_name,str.encode("{}]}"))
        producer.flush()
        producer.close()
       
        print("done")
        
        

if __name__ == "__main__":
    main()
