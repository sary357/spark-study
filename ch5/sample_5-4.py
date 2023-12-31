from pyspark import SparkConf, SparkContext
# This example is a Python version of sample 5-4

folder_path="file:///home/ec2-user/spark/ch5/docs/textfiles"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
lines=sc.wholeTextFiles(folder_path)

# key: file name
# value: file content
print(lines.first())


