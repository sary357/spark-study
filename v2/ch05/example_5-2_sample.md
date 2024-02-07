## P.119-P.120
```
$ ./spark-sql --master spark://`hostname -f`:7077
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/02/07 09:42:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
24/02/07 09:42:44 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
24/02/07 09:42:44 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
24/02/07 09:42:52 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
24/02/07 09:42:52 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore ec2-user@172.17.1.163
24/02/07 09:42:52 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException
Spark Web UI available at http://ip-172-17-1-163.ec2.internal:4040
Spark master: spark://ip-172-17-1-163.ec2.internal:7077, Application Id: app-20240207094241-0009
spark-sql (default)> create table people (name STRING, age INT)
                   > ;
24/02/07 09:45:57 WARN ResolveSessionCatalog: A Hive serde table will be created as there is no table provider specified. You can set spark.sql.legacy.createHiveTableByDefault to false so that native data source table will be created instead.
24/02/07 09:45:58 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
24/02/07 09:45:59 WARN HiveConf: HiveConf of name hive.internal.ss.authz.settings.applied.marker does not exist
24/02/07 09:45:59 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
24/02/07 09:45:59 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
24/02/07 09:45:59 WARN HiveMetaStore: Location: file:/opt/spark/spark-3.5.0-bin-hadoop3/bin/spark-warehouse/people specified for non-external table:people
Time taken: 2.798 seconds
spark-sql (default)> insert into people values ("Michael", NULL);
24/02/07 09:47:01 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Time taken: 5.096 seconds
spark-sql (default)> insert into people values ("Andy", 30);
Time taken: 1.088 seconds
spark-sql (default)> insert into people values ("Samantha",19);
Time taken: 0.72 seconds
spark-sql (default)> show tables;
people
Time taken: 0.317 seconds, Fetched 1 row(s)
spark-sql (default)> select * from people where age >20;
Andy	30
Time taken: 2.294 seconds, Fetched 1 row(s)
spark-sql (default)> select name from people where age is NULL;
Michael
Time taken: 0.516 seconds, Fetched 1 row(s)

```
