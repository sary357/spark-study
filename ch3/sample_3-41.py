#!/usr/bin/env python
# try to show codes on how to persist data with python

from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf, SparkContext

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. load data
lines = sc.parallelize([1,2,3,4,5,6,7])
square = lines.map(lambda x: x*x)

# 3. StorageLevel.MEMORY_ONLY: only keep data in memory
square.persist(StorageLevel.MEMORY_ONLY) # only keep data in memory
print(square.collect())
