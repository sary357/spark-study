from pyspark import SparkConf, SparkContext
# This example is a Python version of sample 6-2

folder_path="file:///home/ec2-user/spark/ch6/docs/inputfile.txt"
outputDir="file:///home/ec2-user/spark/ch6/docs/output/"

# 1. create SparkContext
conf=SparkConf().setMaster("local").setAppName("My App")
sc=SparkContext(conf=conf)

# 2. do some RDD operations
blankLine=sc.accumulator(0)
lines=sc.textFile(folder_path)

def extractCallSigns(line):
    global blankLine
    if line == "":
        blankLine+=1
    return line.split(" ")

callsigns=lines.flatMap(extractCallSigns) # flatMap: transformation
callsigns.saveAsTextFile(outputDir+"/callsigns")
print("Blank lines: "+ str(blankLine.value))
