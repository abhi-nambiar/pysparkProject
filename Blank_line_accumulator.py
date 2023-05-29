import os
import sys
from pyspark import SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def blank_line_check(line):
    if len(line)==0:
        my_accum.add(1)

sc = SparkContext("local[*]", "blank_line_accumulator")

lines = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week10/datasets/samplefile.txt")

my_accum = sc.accumulator(0)
#give 0.0 if you want float

#foreach is available for rdd but not for local variable like list in pyspark
lines.foreach(blank_line_check)

print(my_accum.value)