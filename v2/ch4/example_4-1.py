from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc
# book: P.85

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-4_1").getOrCreate()
    csv_file='data/departuredelays.csv'
    df=spark.read.csv(csv_file, header=True, inferSchema=True,samplingRatio=0.01)
    print("=="*10)
    df.show(2)
    print("=="*10)
    df.createOrReplaceTempView("us_delay_flights_tbl")
 
     # P.86
    print("=="*10)
    spark.sql("select distance,origin,destination from us_delay_flights_tbl where distance > 1000 order by distance desc").show(10)

    # P.87
    print("=="*10)
    spark.sql("select date, delay, origin, destination from us_delay_flights_tbl where origin=\"SFO\" and destination=\"ORD\" and delay>=120 order by delay desc").show(10)
     
    # P.88
    print("=="*10)
    spark.sql("""select delay, origin, destination, 
                    CASE
                 WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                  WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                  WHEN delay > 0 and delay < 60  THEN  'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'Early'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10)
    # P.88
    print("=="*10)
    df.select("distance","origin","destination").where(col("distance")>1000).orderBy(desc("distance")).show(10)

    # P.89
    print("=="*10)
    df.select("date", "delay", "origin", "destination").where((col("origin")=="SFO") & (col("destination")=="ORD") & (col("delay")>=120)).orderBy(desc("delay")).show(10)
