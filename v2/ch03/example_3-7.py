from pyspark.sql import SparkSession

# book: P.59

if __name__ == "__main__":
    file_schema="`Incident Number` INT, `Exposure Number` STRING ,ID STRING,Address STRING,`Incident Date` STRING,`Call Number` STRING,`Alarm DtTm` STRING,`Arrival DtTm` STRING,`Close DtTm` STRING,`City` STRING,`zipcode` STRING,`Battalion` STRING,`Station Area` STRING,`Box` STRING,`Suppression Units` STRING,`Suppression Personnel` STRING,`EMS Units` STRING,`EMS Personnel` STRING,`Other Units` STRING,`Other Personnel` STRING,`First Unit On Scene` STRING,`Estimated Property Loss` STRING,`Estimated Contents Loss` STRING,`Fire Fatalities` STRING,`Fire Injuries` STRING,`Civilian Fatalities` STRING,`Civilian Injuries` STRING,`Number of Alarms` INT,`Primary Situation` STRING,`Mutual Aid` STRING,`Action Taken Primary` STRING,`Action Taken Secondary` STRING,`Action Taken Other` STRING,`Detector Alerted Occupants` STRING,`Property Use` STRING,`Area of Fire Origin` STRING,`Ignition Cause` STRING,`Ignition Factor Primary` STRING,`Ignition Factor Secondary` STRING,`Heat Source` STRING,`Item First Ignited` STRING,`Human Factors Associated with Ignition` STRING,`Structure Type` STRING,`Structure Status` STRING,`Floor of Fire Origin` STRING,`Fire Spread` STRING,`No Flame Spead` STRING,`Number of floors with minimum damage` INT,`Number of floors with significant damage` INT,`Number of floors with heavy damage` INT,`Number of floors with extreme damage` STRING,`Detectors Present` STRING,`Detector Type` STRING,`Detector Operation` STRING,`Detector Effectiveness` STRING,`Detector Failure Reason` STRING,`Automatic Extinguishing System Present` STRING,`Automatic Extinguishing Sytem Type` STRING,`Automatic Extinguishing Sytem Perfomance` STRING,`Automatic Extinguishing Sytem Failure Reason` STRING,`Number of Sprinkler Heads Operating` INT,`Supervisor District` STRING,`neighborhood_district` STRING,`point` STRING" 
    spark=SparkSession.builder.appName("Example-3_7").getOrCreate()
    # sf_fire_df=spark.read.csv("./sf-fire-calls.csv", header=True, inferSchema=True,samplingRatio=0.001)
    sf_fire_df=spark.read.csv("data/sf-fire-calls-sample.csv", schema=file_schema, header=True, inferSchema=False)
    print("=="*10)
    sf_fire_df.show(10)
    print("=="*10)
    sf_fire_df.printSchema()
    print("=="*10)
    print(sf_fire_df.schema)
    print("=="*10)
