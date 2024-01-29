from pyspark import SparkConf, SparkContext

file_path="/home/ec2-user/spark/ch2/README.md"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
lines=sc.textFile(file_path)
# word count
word_count=lines.flatMap(lambda x: x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda y1, y2: y1+y2)
rdd_result=word_count.collect()
print("*"*20+" word count " + "*"*20)
print(rdd_result)
print("*"*52)
