import os
import sys
from pyspark import SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "log_level")

if __name__ == "__main__":
    my_list = ["WARN: Tuesday 4 September 0405",
             "ERROR: Tuesday 4 September 0408",
             "ERROR: Tuesday 4 September 0408",
             "ERROR: Tuesday 4 September 0408",
             "ERROR: Tuesday 4 September 0408",
             "ERROR: Tuesday 4 September 0408"]
    logs_rdd = sc.parallelize(my_list)
else:
    logs_rdd = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week10/datasets/bigLog.txt")
#else condition will ensure that the hardcoded version is not selected in production

rdd2 = logs_rdd.map(lambda x: (x.split(":")[0], 1))

rdd3 = rdd2.reduceByKey(lambda x,y: x + y)

result = rdd3.collect()

for i in result:
    print(i)






