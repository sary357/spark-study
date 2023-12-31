from pyspark import SparkConf, SparkContext
import json
# Sample 5-6

file_path="file:///home/ec2-user/spark/ch5/docs/jsonfiles/01.json"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
lines=sc.textFile(file_path).map(lambda x:json.loads(x))

print(lines.first())


