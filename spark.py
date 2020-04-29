import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./elasticsearch-hadoop-6.1.1/dist/elasticsearch-spark-20_2.11-7.6.2.jar pyspark-shell'
# es_write_conf = {
# "es.nodes" : 'localhost',
# "es.port" : '9200',
# "es.resource" : 'spark/twitter_project',
# "es.input.json" : "yes",
# "es.mapping.id": "doc_id"

# }
import findspark
findspark.init('C:/spark')

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from geopy.geocoders import Nominatim
# from textblob import TextBlob
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import hashlib

TCP_IP = 'localhost'
TCP_PORT = 9001

def sentiment_analysis(tweet):
        # [pos: .xxx, neg: .xxx, neu:. xxx]
        analyzer = SentimentIntensityAnalyzer()
        return analyzer.polarity_scores(tweet)

def es_index(doc):
        es = Elasticsearch()
        es.indices.create(index='spark/twitter_project', ignore=400)
        res = es.index('spark/twitter_project', id=doc['_id'], body=doc)
        #index the tweet and the location
        #es.index()
        print(res)     



def processTweet(tweet):

    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        
        text = tweetData[1]
        rawLocation = tweetData[0]

        sentiment_scores = sentiment_analysis(text)

        # (i) Apply Sentiment analysis in "text"

	# (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation
        geolocator = Nominatim(user_agent="twitter sentiment analysis")
        location = geolocator.geocode(rawLocation)
        print("\n\n=========================\ntweet: ", tweet)
        print("Raw location from tweet status: ", rawLocation)
        print("GeoPy location: ", location)
        
        # print("lat: ", lat)
        # print("lon: ", lon)
        # print("state: ", state)
        # print("country: ", country)
        # print("Text: ", text)
        # print("Sentiment: ", sentiment)



        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        #es_index(sentiment_scores, location)'
        text_hash = abs(int(hashlib.sha256(text.encode('utf-8')).hexdigest()[:8], 16))
        v = {'_id': text_hash, 'text': text, 'location': location, 'sentiment': sentiment_scores}
        print(v)
        #return v    
        



# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")
# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))
#.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(path='-',outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",keyClass="org.apache.hadoop.io.NullWritable",valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",conf=es_write_conf))


ssc.start()
ssc.awaitTermination()
