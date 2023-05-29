import os
import sys
from pyspark import SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def parse_line(line):
    fields = line.split("::")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


sc = SparkContext("local[*]", "avg_friends")

lines = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/friendsdata.csv")

rdd2 = lines.map(parse_line)
#(33,385)

rdd3 = rdd2.mapValues(lambda x: (x, 1))
#(33,(385,1))

#in scala, we used to access elements of tuple using x._1 and x._2
#in python, we access the elements of tuple using x[0] and x[1]

rdd4 = rdd3.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

rdd5 = rdd4.mapValues(lambda x: x[0]/x[1]).sortByKey()
#this will find the average friends

result = rdd5.collect()

for i in result:
    print(i)
