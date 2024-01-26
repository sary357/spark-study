from pyspark import SparkConf, SparkContext
import math
# This example is a Python version of sample 6-19


# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. lookupCountry
distanceNumerics=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
stats=distanceNumerics.stats()
stddev=stats.stdev()
mean=stats.mean()
reasonableDistances=distanceNumerics.filter(
        lambda x: math.fabs(x-mean) < 3 * stddev)
print(reasonableDistances.collect())
print(stddev)
print(mean)
