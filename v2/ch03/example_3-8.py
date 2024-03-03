from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, year

# book: P.64

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-3_7").getOrCreate()


    sf_fire_df=spark.read.parquet("/tmp/sf-fire-calls-parquet")
    sf_fire_df.printSchema()

    # P.64: to_timestamp
    #df_with_timestamp = df.withColumn("date_timestamp", to_timestamp("date_string", "yyyy/MM/dd'T'HH:mm:ss"))
    sf_fire_df_2=sf_fire_df.withColumn("IncidentDate", to_timestamp(("Incident Date"),"yyyy-MM-dd'T'HH:mm:ss"))
    sf_fire_df_2.select("IncidentDate", "Incident Date").show(2, False)


    # P.66
    sf_fire_df_2.select("City","zipcode","IncidentDate").where(col("zipcode").isNotNull()).groupBy(year(col("IncidentDate")),col("City"),col("zipcode")).count().orderBy("count",ascending=False).show(4
