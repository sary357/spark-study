from pyspark import SparkConf, SparkContext
import json
# Sample 5-9
input_file_path="file:///home/ec2-user/spark/ch5/docs/jsonfiles/01.json"
output_file_path="file:///home/ec2-user/spark/ch5/docs/jsonfiles/output"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
lines=sc.textFile(input_file_path).map(lambda x:json.loads(x))

lines.map(lambda x:json.dump(x)).saveAsTextFile(output_file_path)



