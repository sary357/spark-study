from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc
from pyspark.sql.types  import * 

# book: P.142

if __name__ == "__main__":
    spark=SparkSession.builder.appName("Example-5_4").getOrCreate()
    schema=StructType([StructField("celsius", ArrayType(IntegerType()))])
    t_list=[[35,36,32,30,40,42,38]],[[31,32,34,55,56]]
    t_c=spark.createDataFrame(t_list, schema)
    t_c.createOrReplaceTempView("tC")

    t_c.show(truncate=False)

    print("--"*10)

    # P.142-P.143
    spark.sql("select celsius, transform(celsius, t->((t*9) div 5 )+32) as fahrenheit from tC").show(truncate=False)

    # P..143
    spark.sql("select celsius, filter(celsius, t->(t>38)) as high from tC").show(truncate=False)

    # P..143
    spark.sql("select celsius, exists(celsius, t->(t=38)) as threshold from tC").show(truncate=False)

    # P..144
    spark.sql("""select celsius, 
                       reduce(celsius, 
                       0, 
                       (t, acc) -> t+acc, 
                       acc->(acc div size(celsius) * 9 div 5)+32) as avgFahrenheit from tC""").show(truncate=False)

