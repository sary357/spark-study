from pyspark.sql import SparkSession


# sf-fire-calls.csv: data from https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/data
# sf-fire-calls-sample.csv: only 200 records

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-3_7").getOrCreate()
    # sf_fire_df=spark.read.csv("./sf-fire-calls.csv", header=True, inferSchema=True,samplingRatio=0.001)
    sf_fire_df=spark.read.csv("./sf-fire-calls-sample.csv", header=True, inferSchema=True,samplingRatio=1)
    print("=="*10)
    sf_fire_df.show(10)
    print("=="*10)
    sf_fire_df.printSchema()
    print("=="*10)
    print(sf_fire_df.schema)
    print("=="*10)
