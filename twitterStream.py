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
ssc = StreamingContext(sc, 50)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9008
dataStream = ssc.socketTextStream("localhost",9008)
lines = dataStream.window(5000)  


# split each tweet into words
words = lines.flatMap(lambda line: line.split(" "))


# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: w.startswith( '#' ))\
               .map(lambda x: (x, 1))
                
# adding the count of each hashtag to its last count
counts = hashtags.reduceByKey(lambda v1, v2: v1+v2)\
                 .transform(lambda rdd: rdd.sortBy(lambda a: a[1],ascending=False))
counts.pprint()

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
