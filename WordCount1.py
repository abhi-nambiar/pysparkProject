import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "wordcount1")
sc.setLogLevel("ERROR")

ip = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/search_data.txt")
words = ip.flatMap(lambda x: x.split(" "))
word_counts = words.map(lambda x: (x, 1))
final_count = word_counts.reduceByKey(lambda x, y: x + y)
result = final_count.collect()
for a in result:
    print(a)


