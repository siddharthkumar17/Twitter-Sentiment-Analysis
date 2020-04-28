import findspark
findspark.init('C:/spark')

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from geopy.geocoders import Nominatim
# from textblob import TextBlob
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


TCP_IP = 'localhost'
TCP_PORT = 9001

def sentiment_analysis(tweet):

        analyzer = SentimentIntensityAnalyzer()
        return analyzer.polarity_scores(tweet)

def es_index(sentiment, location):
        es = Elasticsearch()
        #index the tweet and the location
        #es.index()


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
        es_index(sentiment_scores, location)     
        



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


ssc.start()
ssc.awaitTermination()
