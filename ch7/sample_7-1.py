from pyspark import SparkConf, SparkContext
# How to submit a spark job?
# command:
#    /opt/spark/latest/bin/spark-submit --master spark://MASTER_NODE_FULL_HOSTNAME:7077 --name "Job_7-1" sample_7-1.py 
# if you need to set max memory size and max cores
#    /opt/spark/latest/bin/spark-submit --master spark://MASTER_NODE_FULL_HOSTNAME:7077 --name "Job_7-1" --deploy-mode client --executor-memory 512mb --executor-cores 1 sample_7-1.py
#    /opt/spark/latest/bin/spark-submit --master spark://MASTER_NODE_FULL_HOSTNAME:7077 --name "Job_7-1" --deploy-mode client --executor-memory 512mb --total-executor-cores 1 sample_7-1.py


file_path="/home/ec2-user/spark/ch2/README.md"

# 1. create SparkContext
conf=SparkConf() #.setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
lines=sc.textFile(file_path)
print(lines.count())
