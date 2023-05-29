import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "wordcount2")

ip = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/search_data.txt")

words = ip.flatMap(lambda x: x.split(" "))

word_counts = words.map(lambda x: (x.upper(), 1))

final_count = word_counts.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0]))

sort_values = final_count.sortByKey(False).map(lambda x: (x[1], x[0]))

result = sort_values.collect()

for a in result:
   print(a)


