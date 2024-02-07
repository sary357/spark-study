## P.114
```
 pyspark --master spark://`hostname -f`:7077
Python 3.9.16 (main, Sep  8 2023, 00:00:00) 
[GCC 11.4.1 20230605 (Red Hat 11.4.1-2)] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/02/07 07:44:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.16 (main, Sep  8 2023 00:00:00)
Spark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240207074421-0002).
SparkSession available as 'spark'.
>>> from pyspark.sql.types import LongType
>>> def cubed(s):
...     return s*s*s
... 
>>> spark.udf.register("cubed", cubed, LongType())
<function cubed at 0x7f2b008e2820>
>>> spark.sql("select id, cubed(id) from udf_test").show()
+---+---------+                                                                 
| id|cubed(id)|
+---+---------+
|  1|        1|
|  2|        8|
|  3|       27|
|  4|       64|
|  5|      125|
|  6|      216|
|  7|      343|
|  8|      512|
+---+---------+


```

## P.116
```
$ pyspark --master spark://`hostname -f`:7077
Python 3.9.16 (main, Sep  8 2023, 00:00:00) 
[GCC 11.4.1 20230605 (Red Hat 11.4.1-2)] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/02/07 09:20:50 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.16 (main, Sep  8 2023 00:00:00)
Spark context Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark context available as 'sc' (master = spark://ip-172-17-1-163.ec2.internal:7077, app id = app-20240207092052-0007).
SparkSession available as 'spark'.
>>> import pandas as pd
>>> from pyspark.sql.functions import col, pandas_udf
>>> from pyspark.sql.types import  LongType
>>> def cubed(a: pd.Series) -> pd.Series:
...     return a*a*a
... 
>>> cubed_udf=pandas_udf(cubed, returnType=LongType())
>>> x=pd.Series([1,2,3])
>>> print(cubed(x))
0     1
1     8
2    27
dtype: int64
>>> df=spark.range(1,4)
>>> df.printSchema()
root
 |-- id: long (nullable = false)

>>> df.select("id",cubed_udf(col("id"))).show()

```
