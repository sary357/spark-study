from pyspark import SparkConf, SparkContext
# This example is a Python version of sample 6-7

input_data_path="file:///home/ec2-user/spark/ch6/docs/6-7_data.txt"
outputDir="file:///home/ec2-user/spark/ch6/docs/output/"
country_list="/home/ec2-user/spark/ch6/docs/6-7_list.txt"

# 0. load CSV
def loadCSVTable(): # this is very similar with loadCallSignTable() in the book
    countryCodeList={}
    with open(country_list) as f:
        for line in f.readlines():
            arr=line.split(",")
            countryCode=arr[0].strip()
            countryName=arr[1].strip()
            countryCodeList[countryCode]=countryName
    return countryCodeList

def lookupCountry(countryCode, countryCodeList):
    return countryCodeList[countryCode]


# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. lookupCountry
countryList=loadCSVTable()
countryPrefixes=sc.broadcast(countryList)

def processCountryCount(countryStats):
    countryFullName = lookupCountry(countryStats[0], countryPrefixes.value)
    count = countryStats[1]
    return (countryFullName, count)

# 3. do some RDD operations
lines=sc.textFile(input_data_path)
countryContactPairs = lines.map(lambda line: line.split(",")).map(lambda pair: (pair[0], int(pair[1])))
countryContactCounts = countryContactPairs.reduceByKey(lambda x, y: x + y).map(processCountryCount)
#contryContactCounts = (lines.map(processContryCount).reduceByKey(lambda x,y: x+y))
countryContactCounts.saveAsTextFile(outputDir+"countryContactCounts")

