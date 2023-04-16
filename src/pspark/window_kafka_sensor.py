from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__=="__main__":
    
    spark = (SparkSession.Builder()
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
             .appName(name='sensor_read_py').getOrCreate())
    
    df = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092") 
                .option("subscribe", "sensor") 
                .option("startingOffsets", "latest")
                .load()
            )
    schema = StructType([StructField('id', IntegerType(), True), StructField('data', DecimalType(10,2), True), StructField('date', StringType(), True)])
    sensor_df = (df.selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"),schema).alias("DATA"))
                .select("DATA.*")
                .withColumn("event_time", to_timestamp(col=col("date"),format="dd-MM-yyyy HH:mm:ss"))
                .drop("time_stamp")
                .where(col("id").isNotNull()))
    
# root
#  |-- id: integer (nullable = true)
#  |-- data: decimal(10,0) (nullable = true)
#  |-- date: string (nullable = true)
#  |-- event_time: timestamp (nullable = true)

    grp_df =(sensor_df
            .groupBy("id",window("event_time","3 seconds"))
            .agg(sum(col("data")).alias("total_temp"))
            ).select("id","total_temp")
    
    (grp_df.writeStream 
      .format("console") 
      .outputMode("update") 
      .option("checkpointLocation", "checkpoint-dir")
      .start() 
      .awaitTermination())
    
    

    
 