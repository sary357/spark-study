from pyspark import SparkConf, SparkContext
import json
import csv
from io import StringIO

# Sample 5-18
input_file_path="file:///home/ec2-user/spark/ch5/docs/csvfiles/01.csv"
output_file_path="file:///home/ec2-user/spark/ch5/docs/csvfiles/output"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations: read
def loadRecord(line):
    input=StringIO(line)
    reader=csv.DictReader(input, fieldnames=["name","favoriteAnimal"])
    return next(reader) #.next()

input = sc.textFile(input_file_path).map(loadRecord)

def writeRecords(records):
    output=StringIO()
    writer=csv.DictWriter(output, fieldnames=["name","favoriteAnimal"])
    for r in records:
        writer.writerow(r)
    return [output.getvalue()]
input.mapPartitions(writeRecords).saveAsTextFile(output_file_path)
