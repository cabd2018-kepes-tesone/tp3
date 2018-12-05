from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio3")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 5)
ssc.checkpoint("buffer")

stream = ssc.socketTextStream("localhost", 7777)

def armarTupla(dato):
    t = dato.split(';')

    return ((int(t[1]), int(t[2])), t[4], 1)

counts = stream \
    .map(armarTupla) \
    .filter(lambda t: t[1] <> '') \
    .map(lambda t: (t[0], t[2])) \
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

history \
    .transform(lambda r: r.sortBy(lambda x: x[1], ascending=False)) \
    .pprint(10)

ssc.start()
ssc.awaitTermination()
