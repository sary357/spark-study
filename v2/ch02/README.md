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

## 1. Launching a Spark master (standalone mode)
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

## 2. run interactive command like `pyspark --master spark://HOSTNAME:7077`
- 
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
>>> 

```
## 3. check Web UI: http://HOSTNAME:8080
