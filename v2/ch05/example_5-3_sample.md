## P.121
```
$ ./beeline 
Beeline version 2.3.9 by Apache Hive
beeline> !connect jdbc:hive2://localhost:10000
Connecting to jdbc:hive2://localhost:10000
Enter username for jdbc:hive2://localhost:10000: 
Enter password for jdbc:hive2://localhost:10000: 
Connected to: Spark SQL (version 3.5.0)
Driver: Hive JDBC (version 2.3.9)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://localhost:10000> show tables;
+------------+------------+--------------+
| namespace  | tableName  | isTemporary  |
+------------+------------+--------------+
| default    | people     | false        |
+------------+------------+--------------+
1 row selected (3.126 seconds)
0: jdbc:hive2://localhost:10000> select  * from people;
+-----------+-------+
|   name    |  age  |
+-----------+-------+
| Michael   | NULL  |
| Andy      | 30    |
| Samantha  | 19    |
+-----------+-------+
3 rows selected (2.712 seconds)

```
