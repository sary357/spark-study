## Sample codes (P.102)

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
```
# header: 第一行是不是欄位名稱, True 代表是
# mode FAILFAST: 如果有錯誤就跳出
>>> df=spark.sql("create or replace temporary view us_delay_flight_tble using csv options (path '/home/ec2-user/spark/ch4/data/departuredelays.csv', header 'True', inferSchema 'true', mode 'FAILFAST')")
>>> spark.sql("select * from us_delay_flight_tble").show(10)
+-------+-----+--------+------+-----------+
|   date|delay|distance|origin|destination|
+-------+-----+--------+------+-----------+
|1011245|    6|     602|   ABE|        ATL|
|1020600|   -8|     369|   ABE|        DTW|
|1021245|   -2|     602|   ABE|        ATL|
|1020605|   -4|     602|   ABE|        ATL|
|1031245|   -4|     602|   ABE|        ATL|
|1030605|    0|     602|   ABE|        ATL|
|1041243|   10|     602|   ABE|        ATL|
|1040605|   28|     602|   ABE|        ATL|
|1051245|   88|     602|   ABE|        ATL|
|1050605|    9|     602|   ABE|        ATL|
+-------+-----+--------+------+-----------+
only showing top 10 rows

>>> df=spark.table("us_delay_flight_tble")
>>> df.write.format("csv").mode("overwrite").save("/tmp/my-location-202402061040")
>>> df.write.format("csv").mode("overwrite").option("compression","bzip2").save("/tmp/my-location-202402061040") # compression: 壓縮方式
```

