import os
import sys
from pyspark import SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def load_boring_words():
   boring_words = set(line.strip() for line in open("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week10/datasets/boringwords.txt"))
   return boring_words

sc = SparkContext("local[*]", "big_data_campaign")

name_set = sc.broadcast(load_boring_words())

rdd1 = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week10/datasets/bigdatacampaign.csv")

rdd2 = rdd1.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

#rdd3 = rdd2.map(lambda x: (x[1], x[0]))

rdd4 = rdd2.flatMapValues(lambda x: x.split(" "))

rdd5 = rdd4.map(lambda x: (x[1].lower(), x[0]))

filtered_words = rdd5.filter(lambda x: x[0] not in name_set.value)

rdd6 = filtered_words.reduceByKey(lambda x, y: x+y)

rdd7 = rdd6.sortBy(lambda x: x[1], False)

result = rdd7.take(20)

for i in result:
    print(i)
