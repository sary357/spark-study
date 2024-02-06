## Sample codes (P.95)

```
$ pyspark --master spark://`hostname`:7077 
Python 3.9.16 (main, Sep  8 2023, 00:00:00) 
[GCC 11.4.1 20230605 (Red Hat 11.4.1-2)] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/02/05 02:28:06 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.16 (main, Sep  8 2023 00:00:00)
Spark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240205022807-0000).
SparkSession available as 'spark'.
>>> file_path="/home/ec2-user/spark/ch4/data/departuredelays.csv"
>>> schema="`date` STRING, `delay` INT, `distance` STRING, `origin` STRING, `destination` STRING"
>>> df=spark.read.format("csv").load(file_path, inferSchema=False, schema=schema,header=True)
>>> df.show(10)
+--------+-----+--------+------+-----------+
|    date|delay|distance|origin|destination|
+--------+-----+--------+------+-----------+
|01011245|    6|     602|   ABE|        ATL|
|01020600|   -8|     369|   ABE|        DTW|
|01021245|   -2|     602|   ABE|        ATL|
|01020605|   -4|     602|   ABE|        ATL|
|01031245|   -4|     602|   ABE|        ATL|
|01030605|    0|     602|   ABE|        ATL|
|01041243|   10|     602|   ABE|        ATL|
|01040605|   28|     602|   ABE|        ATL|
|01051245|   88|     602|   ABE|        ATL|
|01050605|    9|     602|   ABE|        ATL|
+--------+-----+--------+------+-----------+
only showing top 10 rows
```

## Save with the format `parquet` (P.97)
```
>>> df.write.format("parquet").mode("overwrite").save("/tmp/my-location")
>>> df1=spark.read.format("parquet").load("/tmp/my-location")
>>> df1.show(10)
+--------+-----+--------+------+-----------+
|    date|delay|distance|origin|destination|
+--------+-----+--------+------+-----------+
|01011245|    6|     602|   ABE|        ATL|
|01020600|   -8|     369|   ABE|        DTW|
|01021245|   -2|     602|   ABE|        ATL|
|01020605|   -4|     602|   ABE|        ATL|
|01031245|   -4|     602|   ABE|        ATL|
|01030605|    0|     602|   ABE|        ATL|
|01041243|   10|     602|   ABE|        ATL|
|01040605|   28|     602|   ABE|        ATL|
|01051245|   88|     602|   ABE|        ATL|
|01050605|    9|     602|   ABE|        ATL|
+--------+-----+--------+------+-----------+
only showing top 10 rows


```

## Use the data with the format `parquet`. (P.98-P.99)
```
>>> spark.sql("create or replace temporary view us_delay_flight_tbl using parquet options( path '/tmp/my-location')")
DataFrame[]
>>> spark.sql("select * from us_delay_flight_tbl ").show(10)
+--------+-----+--------+------+-----------+
|    date|delay|distance|origin|destination|
+--------+-----+--------+------+-----------+
|01011245|    6|     602|   ABE|        ATL|
|01020600|   -8|     369|   ABE|        DTW|
|01021245|   -2|     602|   ABE|        ATL|
|01020605|   -4|     602|   ABE|        ATL|
|01031245|   -4|     602|   ABE|        ATL|
|01030605|    0|     602|   ABE|        ATL|
|01041243|   10|     602|   ABE|        ATL|
|01040605|   28|     602|   ABE|        ATL|
|01051245|   88|     602|   ABE|        ATL|
|01050605|    9|     602|   ABE|        ATL|
+--------+-----+--------+------+-----------+
only showing top 10 rows

>>> df=spark.sql("select * from us_delay_flight_tbl ")
>>> df.write.format("parquet").option("compression","snappy").save("/tmp/my-location1")
```

## Save the data to Spark SQL tables (P.99)
```
>>> df.write.mode("overwrite").saveAsTable("us_delay_flight_tbl_1")
24/02/05 03:07:44 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
24/02/05 03:07:45 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
24/02/05 03:07:45 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
24/02/05 03:07:45 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
>>> spark.sql("select * from us_delay_flight_tbl_1").show(10)
+--------+-----+--------+------+-----------+
|    date|delay|distance|origin|destination|
+--------+-----+--------+------+-----------+
|01011245|    6|     602|   ABE|        ATL|
|01020600|   -8|     369|   ABE|        DTW|
|01021245|   -2|     602|   ABE|        ATL|
|01020605|   -4|     602|   ABE|        ATL|
|01031245|   -4|     602|   ABE|        ATL|
|01030605|    0|     602|   ABE|        ATL|
|01041243|   10|     602|   ABE|        ATL|
|01040605|   28|     602|   ABE|        ATL|
|01051245|   88|     602|   ABE|        ATL|
|01050605|    9|     602|   ABE|        ATL|
+--------+-----+--------+------+-----------+
only showing top 10 rows


```
