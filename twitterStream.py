from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import string




# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 60 seconds
ssc = StreamingContext(sc, 60)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9008
dataStream = ssc.socketTextStream("localhost",9008)
lines = dataStream.window(10000)  


# split each tweet into words
# ***** TODO ******

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
# ***** TODO ******
                
# adding the count of each hashtag to its last count
# ***** TODO ******

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
