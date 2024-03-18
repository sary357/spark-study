## Sample codes (P.109)

```
$ pyspark --master spark://`hostname`:7077
Python 3.9.16 (main, Sep  8 2023, 00:00:00) 
[GCC 11.4.1 20230605 (Red Hat 11.4.1-2)] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/02/06 07:55:34 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.16 (main, Sep  8 2023 00:00:00)
Spark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240206075535-0003).
SparkSession available as 'spark'.
>>> from pyspark.ml import image
>>> image_folders="/home/ec2-user/spark/ch04/images"
>>> image_df=spark.read.format("image").load(image_folders)
>>> image_df.printSchema()
root
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nChannels: integer (nullable = true)
 |    |-- mode: integer (nullable = true)
 |    |-- data: binary (nullable = true)
>>> image_df.select("image.origin", "image.height","image.width","image.nChannels").show(1, truncate=False)
+----------------------------------------------------------+------+-----+---------+
|origin                                                    |height|width|nChannels|
+----------------------------------------------------------+------+-----+---------+
|file:///home/ec2-user/spark/ch04/images/home-simpson-2.jpeg|1390  |674  |3        |
+----------------------------------------------------------+------+-----+---------+
only showing top 1 row

>>> image_df.select("image.origin", "image.height","image.width","image.nChannels","image.mode").show(1, truncate=False)
+----------------------------------------------------------+------+-----+---------+----+
|origin                                                    |height|width|nChannels|mode|
+----------------------------------------------------------+------+-----+---------+----+
|file:///home/ec2-user/spark/ch04/images/home-simpson-2.jpeg|1390  |674  |3        |16  |
+----------------------------------------------------------+------+-----+---------+----+
only showing top 1 row


```
- P.110
```
>>> image_df.printSchema()
root
 |-- path: string (nullable = true)
 |-- modificationTime: timestamp (nullable = true)
 |-- length: long (nullable = true)
 |-- content: binary (nullable = true)

## "pathGlobFilter": means we'd like to filter jpeg files
>>> image_df=spark.read.format("binaryFile").option("pathGlobFilter","*jpeg").option("recursiveFileLookup","true").load(image_folders)
>>> image_df.select("path","modificationTime","length").show(2,truncate=False)
+--------------------------------------------------------+-----------------------+------+
|path                                                    |modificationTime       |length|
+--------------------------------------------------------+-----------------------+------+
|file:/home/ec2-user/spark/ch04/images/home-simpson-2.jpeg|2024-02-06 03:03:10.735|59859 |
|file:/home/ec2-user/spark/ch04/images/home-simpson-1.jpeg|2024-02-06 03:03:11.755|7565  |
+--------------------------------------------------------+-----------------------+------+

>>>
```
