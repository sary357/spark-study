```
## P.55-P.56 
## Rewritten by Python
 
>>> from  pyspark.sql import functions
>>> schema="`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING> "
>>>
>>> data= [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
...            [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
...            [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
...            [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
...            [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
...            [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
...           ]
>>>
>>> blog_df=spark.createDataFrame(data, schema)
>>> blog_df.printSchema()
root
 |-- Id: integer (nullable = true)
 |-- First: string (nullable = true)
 |-- Last: string (nullable = true)
 |-- Url: string (nullable = true)
 |-- Published: string (nullable = true)
 |-- Hits: integer (nullable = true)
 |-- Campaigns: array (nullable = true)
 |    |-- element: string (containsNull = true)

>>> blog_df.show(10,False)
+---+---------+-------+-----------------+---------+-----+----------------------------+
|Id |First    |Last   |Url              |Published|Hits |Campaigns                   |
+---+---------+-------+-----------------+---------+-----+----------------------------+
|1  |Jules    |Damji  |https://tinyurl.1|1/4/2016 |4535 |[twitter, LinkedIn]         |
|2  |Brooke   |Wenig  |https://tinyurl.2|5/5/2018 |8908 |[twitter, LinkedIn]         |
|3  |Denny    |Lee    |https://tinyurl.3|6/7/2019 |7659 |[web, twitter, FB, LinkedIn]|
|4  |Tathagata|Das    |https://tinyurl.4|5/12/2018|10568|[twitter, FB]               |
|5  |Matei    |Zaharia|https://tinyurl.5|5/14/2014|40578|[web, twitter, FB, LinkedIn]|
|6  |Reynold  |Xin    |https://tinyurl.6|3/2/2015 |25568|[twitter, LinkedIn]         |
+---+---------+-------+-----------------+---------+-----+----------------------------+



## we can use expr and col to calculate
## expr = col
>>> blog_df.select('Id',functions.expr('Hits *2')).show()
+---+----------+
| Id|(Hits * 2)|
+---+----------+
|  1|      9070|
|  2|     17816|
|  3|     15318|
|  4|     21136|
|  5|     81156|
|  6|     51136|
+---+----------+
>>> blog_df.select('Id',functions.col('Hits')*2).show()
+---+----------+
| Id|(Hits * 2)|
+---+----------+
|  1|      9070|
|  2|     17816|
|  3|     15318|
|  4|     21136|
|  5|     81156|
|  6|     51136|
+---+----------+



## Add a new column "Big Hitter" and show Hits > 10000
>>> blog_df.withColumn("Big Hitter", functions.expr("Hits > 10000")).show()
+---+---------+-------+-----------------+---------+-----+--------------------+----------+
| Id|    First|   Last|              Url|Published| Hits|           Campaigns|Big Hitter|
+---+---------+-------+-----------------+---------+-----+--------------------+----------+
|  1|    Jules|  Damji|https://tinyurl.1| 1/4/2016| 4535| [twitter, LinkedIn]|     false|
|  2|   Brooke|  Wenig|https://tinyurl.2| 5/5/2018| 8908| [twitter, LinkedIn]|     false|
|  3|    Denny|    Lee|https://tinyurl.3| 6/7/2019| 7659|[web, twitter, FB...|     false|
|  4|Tathagata|    Das|https://tinyurl.4|5/12/2018|10568|       [twitter, FB]|      true|
|  5|    Matei|Zaharia|https://tinyurl.5|5/14/2014|40578|[web, twitter, FB...|      true|
|  6|  Reynold|    Xin|https://tinyurl.6| 3/2/2015|25568| [twitter, LinkedIn]|      true|
+---+---------+-------+-----------------+---------+-----+--------------------+----------+



## create a new column "Author Id" with the columns "First, "Last", and "Id"
>>> blog_df.withColumn("Author Id", functions.concat('First', 'Last', 'Id')).select('Author Id').show()
+-------------+
|    Author Id|
+-------------+
|  JulesDamji1|
| BrookeWenig2|
|    DennyLee3|
|TathagataDas4|
|MateiZaharia5|
|  ReynoldXin6|
+-------------+



## expr = col = write column name directly
>>> blog_df.select("Hits").show(2)
+----+
|Hits|
+----+
|4535|
|8908|
+----+
only showing top 2 rows
>>> blog_df.select(functions.expr("Hits")).show(2)
+----+
|Hits|
+----+
|4535|
|8908|
+----+
only showing top 2 rows

>>> blog_df.select(functions.col("Hits")).show(2)
+----+
|Hits|
+----+
|4535|
|8908|
+----+
only showing top 2 rows


## sort by Hits in descending order
>>> blog_df.sort(functions.col('Hits').desc()).show()
+---+---------+-------+-----------------+---------+-----+--------------------+
| Id|    First|   Last|              Url|Published| Hits|           Campaigns|
+---+---------+-------+-----------------+---------+-----+--------------------+
|  5|    Matei|Zaharia|https://tinyurl.5|5/14/2014|40578|[web, twitter, FB...|
|  6|  Reynold|    Xin|https://tinyurl.6| 3/2/2015|25568| [twitter, LinkedIn]|
|  4|Tathagata|    Das|https://tinyurl.4|5/12/2018|10568|       [twitter, FB]|
|  2|   Brooke|  Wenig|https://tinyurl.2| 5/5/2018| 8908| [twitter, LinkedIn]|
|  3|    Denny|    Lee|https://tinyurl.3| 6/7/2019| 7659|[web, twitter, FB...|
|  1|    Jules|  Damji|https://tinyurl.1| 1/4/2016| 4535| [twitter, LinkedIn]|
+---+---------+-------+-----------------+---------+-----+--------------------+

```

-----
```
### P.57
## Row is an indexed object 
## blog_row[0] => 6
## blog_row[1] => "Reynold"
>>> blog_row=Row(6, "Reynold", "Xin", "https://tinyurl.6", 25568, "3/2/2015", ["twitter","LinkedIn"])
>>> blog_row[1]
'Reynold'

## we can create a DataFrame from multiple Rows 
>>> rows=[Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
>>> authors_df=spark.createDataFrame(rows, ["Author", "State"])
>>> authors_df.show()
+-------------+-----+
|       Author|State|
+-------------+-----+
|Matei Zaharia|   CA|
|  Reynold Xin|   CA|
+-------------+-----+

```
