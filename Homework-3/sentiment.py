# Name: Aditya Viswanatham
# NetID: arv160730
# CS 6350.001 HW3

import sys
import json
from pyspark import SparkContext, SparkConf
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from textblob import TextBlob

esConn = Elasticsearch()

def getHashTag(text):
    if "trump" in text.lower():
        return "#trump"
    else:
        return "#corona"

def getSentimentValue(text):
    sentimentAnalyser = SentimentIntensityAnalyzer()
    polarity = sentimentAnalyser.polarity_scores(text)
    if(polarity["compound"] > 0):
        return "positive"
    elif(polarity["compound"] < 0):
        return "negative"
    else:
        return "neutral"

def getSentiment(time, rdd):
    test = rdd.collect()
    for i in test:
        esConn.index(index="hash_tags_sentiment_analysis",
                     doc_type="tweet-sentiment-analysis", body=i)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sentimentAnalysis.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)
    spark = SparkSession \
        .builder \
        .appName("SentimentAnalysis") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 20)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(
        ssc, [topic], {"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: str(x[1].encode("ascii", "ignore"))).map(
        lambda x: (x, getSentimentValue(x), getHashTag(x))).map(lambda x: {"message": x[0], "sentiment": x[1], "hashTag": x[2]})
    tweets.foreachRDD(getSentiment)
    ssc.start()
    ssc.awaitTermination()
