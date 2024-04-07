from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc,expr
from pyspark.sql.types  import * 

# book: P.146

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-5_5").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    tripdelayFilePath="./departuredelays.csv"
    airportsnaFilePath="./airport-codes-na.txt"

    airportsna=spark.read.csv(airportsnaFilePath, header=True, inferSchema=True, sep="\t")
    airportsna.createOrReplaceTempView("airport_na")

    departureDelays=spark.read.csv(tripdelayFilePath, header=True)
    departureDalays=departureDelays.withColumn("delay", expr("CAST(delay as INT) as delay")).withColumn("distance", expr("CAST(distance as INT) as distance"))

    departureDelays.createOrReplaceTempView("departureDelays")

    foo = departureDelays.filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0"""))
    foo.createOrReplaceTempView("foo")

    spark.sql("select * from airport_na limit 10").show(10)
    spark.sql("select * from departureDelays limit 10").show(10)
    spark.sql("select * from foo limit 10").show(10)

    # P. 148
    bar=departureDelays.union(foo)
    bar.createOrReplaceTempView("bar")

    bar.show(10) # == departureDelays
    bar.filter("""origin == 'SEA' and destination == 'SFO' and date like '0101%' and delay > 0 """).show()
    spark.sql("""select * from bar where origin = 'SEA' and destination = 'SFO' and date like '0101%' and delay >0 """).show()

    # P.148-P.149
    # Please keep in mind: all fields in spark.sql/dataframe are case-sensitive. We're supposed to be careful to its field name
    foo.join(airportsna, airportsna.IATA==foo.origin).select('City', 'State','date','delay','distance','destination').show()
    spark.sql("""select a.City, a.State, f.date, f.delay, f.distance, f.destination from  foo f join airport_na a on a.IATA=f.origin """).show()

    # P.150
    spark.sql(""" drop table if exists departureDelaysWindow; """)
    # the following needs Hive support
    #spark.sql(""" create table departureDelaysWindow 
    #        select origin, destination, sum(delay) as TotalDelays 
    #        from departureDelays
    #        where origin in ('SEA', 'SFO', 'JFK') and destination in ('SEA','SFO','JFK','DEN','ORD','LAX','ATL')
    #        group by origin, destination; """)
    #spark.sql("""select * from departureDelaysWindow;""").show()
    departureDelaysWindow=spark.sql("""
            select origin, destination, sum(delay) as TotalDelays 
            from departureDelays
            where origin in ('SEA', 'SFO', 'JFK') and destination in ('SEA','SFO','JFK','DEN','ORD','LAX','ATL')
            group by origin, destination; """)
    
    departureDelaysWindow.createOrReplaceTempView("departureDelaysWindow")
    spark.sql("select * from departureDelaysWindow").show()

    # P.151
    #spark.sql("""
    #select origin. destination, TotalDelays, rank from (
    #    select origin, destination, TotalDelays, dense_rank() over (partition by origin order by TotalDelays desc) as rank
    #        from departureDelaysWindow) t where rank <= 3
    #            """).show()

    # P.152
    foo.show()
    foo2=foo.withColumn("status", 
                        expr("CASE WHEN delay<=10 THEN 'On-time' ELSE 'Delayed' END"))
    foo2.show()

    # P.153
    foo3=foo2.drop("delay")
    foo3.show()

    # P.153
    foo4=foo3.withColumnRenamed("status","flight_status")
    foo4.show()

    # P.153-P.154
    spark.sql("""select destination, CAST(SUBSTRING(date, 0, 2) as int) as month,  delay
                 from departureDelays where origin='SEA'
    """).show()

    # P.154
    spark.sql("""select * from 
                 (select destination, CAST(SUBSTRING(date, 0, 2) as int) as month,  delay
                 from departureDelays where origin='SEA')
                 pivot (
                     cast(avg(delay) as decimal(4,2)) as AvgDelay, max(delay) as MaxDelay for month in (1 Jan, 2 Feb)
                 ) order by destination
    """).show()
