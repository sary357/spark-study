from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc
# book: P.90

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-4_2").getOrCreate()
    print("=="*10)
   
    spark.sql("create database learn_spark_db")
    spark.sql("use learn_spark_db")

    # create a managed table
    csv_file='data/departuredelays.csv'
    flight_df=spark.read.csv(csv_file, header=True, inferSchema=True, samplingRatio=0.01)
    flight_df.write.saveAsTable("managed_us_delay_flights_tbl")
