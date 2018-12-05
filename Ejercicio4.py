from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio4")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 5)
ssc.checkpoint("buffer")

stream = ssc.socketTextStream("localhost", 7777)

franja = int(sys.argv[1])

def armarTupla(dato):
    t = dato.split(';')
    timestamp = int(t[3])
    lim_inferior = (timestamp/franja)*franja
    lim_superior = lim_inferior+franja

    return ((lim_inferior, lim_superior, int(t[0])), 1)

counts = stream.map(armarTupla) \
    .reduceByKey(lambda a, b: a) \
    .map(lambda t: ((t[0][0], t[0][1]), 1)) \
    .reduceByKey(lambda a, b: a + b)

def fUpdate(newValues, history):
    if (history == None):
        history = 0

    if (newValues == None):
        newValues = 0
    else:
        newValues = sum(newValues)

    return newValues + history

history = counts.updateStateByKey(fUpdate)

history.pprint()

ssc.start()
ssc.awaitTermination()
