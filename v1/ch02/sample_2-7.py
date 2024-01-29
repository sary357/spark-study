from pyspark import SparkConf, SparkContext

file_path="/home/ec2-user/spark/ch2/README.md"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
lines=sc.textFile(file_path)
print(lines.count())
