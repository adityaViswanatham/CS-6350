# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW3

import sys
from kafka import KafkaProducer
import tweepy

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        if hasattr(status, "retweeted_status"):
            pass
        else:
            if hasattr(status, "extended_tweet"):
                text = status.extended_tweet["full_text"]
            else:
                text = status.text
            prod = connect_kafka_producer()
            publish_message(prod, 'tweets', text)

    def on_error(self, status_code):
        print("Encountered streaming error with code ", status_code, "\n")
        sys.exit()

def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def connectToTwitterAPI(lst):
    if(len(lst) > 0):
        consumerKey = lst[0]
        consumerSecret = lst[1]
        accessToken = lst[2]
        accessTokenSecret = lst[3]

        listOfHashTags = []
        listOfHashTags.append(lst[4])

        auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
        auth.set_access_token(accessToken, accessTokenSecret)
        api = tweepy.API(auth)

        streamListener = StreamListener()
        stream = tweepy.Stream(
            auth=api.auth, listener=streamListener, tweet_mode='extended')
        stream.filter(track=listOfHashTags)
    else:
        print("Please enter some terms to be searched for\n")
        getInputs()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        exit(-1)
    listOfInputs = sys.argv[1:]
    connectToTwitterAPI(listOfInputs)
