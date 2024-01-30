# How to run a Spark program in standalone mode
## 0. generate ssh key
- Step 1: run the following command
```
$ ssh-keygen

```
- Step 2: append the content from `~/.ssh/id_rsa.pub` to `~/.ssh/authorized_keys`
```
$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys 
```

## 1. Launch a Spark master (standalone mode)
- Assume spark is installed in the folder: `/opt/spark/spark-3.5.0-bin-hadoop3`
- Step 1: go to spark folder
```
$ cd /opt/spark/spark-3.5.0-bin-hadoop3
```
- Step 2: go to folder `sbin`
```
$ cd sbin
```
- Step 3: execute the following command:
```
$ sh start-all.sh
```
- Step 4. check Web UI (Spark master URL): http://HOSTNAME:8080

## 2. Run interactive commands `pyspark` or `spark-shell`, then verify
- Step 1: execute the command `pyspark` or `spark-shell` with `--master spark://HOSTNAME:7077`
```
$ pyspark --master spark://`hostname`:7077
Python 3.9.16 (main, Sep  8 2023, 00:00:00) 
[GCC 11.4.1 20230605 (Red Hat 11.4.1-2)] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/01/29 06:56:32 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.16 (main, Sep  8 2023 00:00:00)
Spark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240129065634-0001).
SparkSession available as 'spark'.
>>> spark.version
'3.5.0' 
```
or
```
$ spark-shell --master spark://`hostname`:7077
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/01/29 07:06:47 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240129070648-0002).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/
         
Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 17.0.10)
Type in expressions to have them evaluated.
Type :help for more information.

scala> spark.version
res0: String = 3.5.0
```
- Step 2: check Web UI: http://HOSTNAME:8080 (Spark master URL) and http://HOSTNAME:4040/ (Job status URL)

-----

# sample codes
## spark-shell:
```
scala> val strings=spark.read.text("/home/ec2-user/spark/ch2/README.md")
strings: org.apache.spark.sql.DataFrame = [value: string]

scala> strings.show(10,false)
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                            
...
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 10 rows


scala> strings.count()
res2: Long = 18
scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> val strings=spark.read.text("/home/ec2-user/spark/ch2/README.md")
strings: org.apache.spark.sql.DataFrame = [value: string]

scala> val filtered=strings.filter(col("value").contains("Hilton"))
filtered: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [value: string]

scala> filtered.count()
res0: Long = 6                                                                  
```

## pyspark
```
$ pyspark --master spark://`hostname`:7077
Python 3.9.16 (main, Sep  8 2023, 00:00:00) 
[GCC 11.4.1 20230605 (Red Hat 11.4.1-2)] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/01/29 07:15:56 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
linWelcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.16 (main, Sep  8 2023 00:00:00)
eSpark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240129071557-0003).
SparkSession available as 'spark'.
>>> lines=sc.textFile('/home/ec2-user/spark/ch2')
>>> strings=spark.read.text('/home/ec2-user/spark/ch2/README.md')
>>> strings.show(10,truncate=False)
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| .....                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 10 rows

>>> strings.count()
18                                                                              
>>> 
>>> strings=spark.read.text('/home/ec2-user/spark/ch2/README.md')
>>> filster=strings.filter(strings.value.contains("Hilton"))
>>> filster.count()
6
```
