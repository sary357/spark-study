from pyspark import SparkConf, SparkContext
import json
import csv
from io import StringIO

# Sample 5-12
input_file_path="file:///home/ec2-user/spark/ch5/docs/csvfiles/01.csv"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
def loadRecord(line):
    input=StringIO(line)
    reader=csv.DictReader(input, fieldnames=["name","favoriteAnimal"])
    return next(reader) #.next()

input = sc.textFile(input_file_path).map(loadRecord)
print("*"*20)
print(input.first())
print("*"*20)
