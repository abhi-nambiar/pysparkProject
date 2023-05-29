import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "cust_orders")

ip = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/customerorders.csv")

rdd2 = ip.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

rdd4 = rdd3.sortBy(lambda x: x[1], False)

result = rdd4.collect()

for i in result:
    print(i)
