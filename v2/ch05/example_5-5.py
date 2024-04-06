from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc,expr
from pyspark.sql.types  import * 

# book: P.146

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-5_5").getOrCreate()
    tripdelayFilePath="./departuredelays.csv"
    airportsnaFilePath="./airport-codes-na.txt"

    airportsna=spark.read.csv(airportsnaFilePath, header=True, inferSchema=True, sep="\t")
    airportsna.createOrReplaceTempView("airport_na")

    departureDelays=spark.read.csv(tripdelayFilePath, header=True)
    departureDalays=departureDelays.withColumn("delay", expr("CAST(delay as INT) as delay")).withColumn("distance", expr("CAST(distance as INT) as distance"))

    departureDelays.createOrReplaceTempView("departureDelays")

    foo = departureDelays.filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0"""))
    foo.createOrReplaceTempView("foo")

    spark.sql("select * from airport_na").show(
