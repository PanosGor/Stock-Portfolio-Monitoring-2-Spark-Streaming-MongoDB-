from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from statistics import mean
from pymongo import MongoClient
from datetime import datetime
import json

sc = SparkContext("local[2]", "Stock_Monitoring")

ssc = StreamingContext(sc, 15)

#get stocks for each interval
stocks = ssc.socketTextStream("localhost", 9999)
stocks.pprint()

#Print the number of new stocks received by the server
stocks.count().map(lambda x:f'Number of new updates received by the server {x}').pprint()


data = stocks.map(lambda v: json.loads(v))


#Print the list of all tickers received in ascending order
tickers = data.map(lambda x:x['TICK']).transform(lambda rdd: rdd.sortBy(lambda x: x))
tickers.pprint()

#Print min,max and mean of each stock received in the interval
data_n =  data.map(lambda x:(x['TICK'],float(x['PRICE']))).groupByKey().mapValues(list)
data_n.map(lambda x:f"Stock: {x[0]}, MAX Price {max(x[1])}").pprint()
data_n.map(lambda x:f"Stock: {x[0]}, MIN Price {min(x[1])}").pprint()
data_n.map(lambda x:f"Stock: {x[0]}, MEAN Price {round(mean(x[1]),2)}").pprint()


#============================================================================================
#Part 2

stocks_c = stocks.window(windowDuration=120,slideDuration=(120))
#stocks.count().map(lambda x:f'Number of new updates received by the server {x}').pprint()
stocks_c.pprint(100)
stocks_c.count().map(lambda x:f'Number of new updates received by the server in the last 2 min {x}').pprint()

data = stocks_c.map(lambda x:json.loads(x))
data_n =  data.map(lambda x:(x['TICK'],float(x['PRICE']))).groupByKey().mapValues(list)
data_n2 = data_n.map(lambda x:(x[0],round((max(x[1])-min(x[1]))/mean(x[1]),2)))
#ThE following line prints data_n2 in order for you to double check the Spread calculations manually
#if you don't want it to appear on the screen comment it out
data_n.pprint(100)
data_n3 = data_n2.map(lambda x:f"Stock: {x[0]}, Spread : {x[1]}")
data_n3.pprint(100)


def maxOverRDD(input_rdd):
    if not input_rdd.isEmpty():
        reduced_rdd = input_rdd.reduce(lambda acc, value : value if (acc[1] < value[1]) else acc)
        internal_result = input_rdd.filter(lambda x: x[1] == reduced_rdd[1])
        return internal_result
    
def minOverRDD(input_rdd):
    if not input_rdd.isEmpty():
        reduced_rdd = input_rdd.reduce(lambda acc, value : value if (acc[1] > value[1]) else acc)
        internal_result = input_rdd.filter(lambda x: x[1] == reduced_rdd[1])
        return internal_result
    
result_max = data_n2.transform(maxOverRDD)
result_max.map(lambda x:f"Stock :{x[0]} with Maximum spread : {x[1]}").pprint()

result_min = data_n2.transform(minOverRDD)
result_min.map(lambda x:f"Stock :{x[0]} with Minimum spread : {x[1]}").pprint()

#===========================================================================
#Part 3

def keep_track(an_RDD):
    data = an_RDD.map(lambda v: json.loads(v))
    for i in data.collect():
        information.insert_one({'TICK':i['TICK'],'PRICE':float(i['PRICE']),'TS':datetime.strptime(i['TS'],'%Y-%m-%d %H:%M:%S.%f')})

        
mdb_client = MongoClient('localhost',27017)
mydb = mdb_client['itc6107']
information = mydb.StockExachange

stocks_mongodb = stocks.foreachRDD(lambda rdd:keep_track(rdd))

ssc.start()   
ssc.awaitTermination() 

