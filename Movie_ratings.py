import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "movie_ratings")

ip = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/moviedata.data")

rating = ip.map(lambda x: (x.split("\t")[2], 1))

agg_ratings = rating.reduceByKey(lambda x, y: x + y)

sort_ratings = agg_ratings.sortByKey()

result = sort_ratings.collect()

for i in result:
    print(i)
