from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys

conf = SparkConf().setMaster("local[2]").setAppName("Ejercicio2")
sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 5)
ssc.checkpoint("buffer")

stream = ssc.socketTextStream("localhost", 7777)

def armarTupla (dato):
    t = dato.split(';')
    return (t[4], 1)

counts = stream.map(armarTupla) \
    .filter(lambda t: t[0] <> '' and t[0] <> 'Otro') \
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

# history.transform(lambda r: r.sortBy(lambda x: x[1], ascending=False)) \
#     .pprint(3)

history.pprint()

ssc.start()
ssc.awaitTermination()
