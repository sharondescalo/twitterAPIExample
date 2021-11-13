import requests
import os
import json
from kafka import KafkaProducer
import traceback
import urllib3
# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    try:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
        return response.json()

    except Exception as e:
        print (e)


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    
    try:
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print(json.dumps(response.json()))
    except Exception as e:
        print (e)


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "#DevOps"},
        # {"value": "dog has:images", "tag": "dog pictures"},
        # {"value": "cat has:images -grumpy", "tag": "cat pictures"},
    ]
    payload = {"add": sample_rules}
    try:
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
    
    except Exception as e:
        print (e)


def get_stream(set):
    try:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
        )
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            hastag =""
            hashtag_list = []
            if response_line:
                json_response = json.loads(response_line)
                words = json_response['data']['text'].split()
                for current_word in words:
                    if current_word.startswith('@'):
                        hastag = current_word
                    if current_word.startswith('#'):
                        hashtag_list.append(current_word)
                    
                # res = json.dumps(json_response['data'], indent=4, sort_keys=True)
                response  = {
                    "TwitterID" : json_response['data']['id'],
                    "Username" : hastag,
                    "Hashtags" : hashtag_list
                }
                #Stream Data to Kafka
            
                # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                
                # producer = KafkaProducer(bootstrap_servers=['localhost:9095','localhost:9094','localhost:9093'],
                producer = KafkaProducer(bootstrap_servers=['kafka1:9092','kafka2:9092','kafka3:9092'],
                value_serializer=lambda m: json.dumps(m).encode('utf-8'))
                producer.send('test-topic',response)
                # print(response)
    except Exception as e:
        print (e)
        print(traceback.format_exc())

    except requests.exceptions.ChunkedEncodingError as e:
        print (e)
        print(traceback.format_exc())
        main()
        
    except urllib3.exceptions.ProtocolError as e:
        print (e)
        print(traceback.format_exc())
        main()

def main():
        rules = get_rules()
        delete = delete_all_rules(rules)
        set = set_rules(delete)
        get_stream(set)      

if __name__ == "__main__":
    main()