## Sample codes (P.100)
### JSON

```
>>> spark.read.format("json").load("/home/ec2-user/spark/ch04/data/json")
DataFrame[DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string, count: bigint]
>>> df2=spark.read.format("json").load("/home/ec2-user/spark/ch04/data/json")
>>> df2.show(10)
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|    1|
|    United States|            Ireland|  264|
|    United States|              India|   69|
|            Egypt|      United States|   24|
|Equatorial Guinea|      United States|    1|
|    United States|          Singapore|   25|
|    United States|            Grenada|   54|
|       Costa Rica|      United States|  477|
|          Senegal|      United States|   29|
|    United States|   Marshall Islands|   44|
+-----------------+-------------------+-----+
only showing top 10 rows

```
### read JSON files into SQL tables (p.100)
```
>>> spark.sql("create or replace temporary view us_delay_flight_tbl_1 using json options (path '/home/ec2-user/spark/ch04/data/json')")
DataFrame[]
>>> spark.sql("select * from us_delay_flight_tbl_1").show(10)
+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|    1|
|    United States|            Ireland|  264|
|    United States|              India|   69|
|            Egypt|      United States|   24|
|Equatorial Guinea|      United States|    1|
|    United States|          Singapore|   25|
|    United States|            Grenada|   54|
|       Costa Rica|      United States|  477|
|          Senegal|      United States|   29|
|    United States|   Marshall Islands|   44|
+-----------------+-------------------+-----+
only showing top 10 rows


```

### Write dataframes to JSON files (P.101)
```
>>> df=spark.sql("select * from us_delay_flight_tbl_1")
>>> df.write.format("json").mode("overwrite").option("compression","snappy").save("/tmp/my-location3")
```
