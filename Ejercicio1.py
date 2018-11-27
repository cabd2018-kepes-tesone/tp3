from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio1")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 5)
ssc.checkpoint("buffer")

stream = ssc.socketTextStream("localhost", 7777)

def armarTupla (dato):
    t = dato.split(';')
    return (int(t[0]), 1 if t[4] <> '' else 0)

counts = stream.map(armarTupla) \
    .filter(lambda t: t[1] > 0) \
    .reduceByKey(lambda a, b: a + b)

def fUpdate (newValues , history):
    if(history == None):
        history = 0
    if( newValues == None):
        newValues = 0
    else:
        newValues = sum( newValues )
    return  newValues +  history

history = counts.updateStateByKey ( fUpdate )

history.pprint()

ssc.start()
ssc.awaitTermination()
