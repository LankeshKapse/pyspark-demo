from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__=="__main__":
    
    spark = (SparkSession.Builder()
             .config("spark.sql.shuffle.partitions", "10")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
             .appName(name='sensor_read_py').getOrCreate())
    
    df = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092") 
                .option("subscribe", "sensor") 
                .option("startingOffsets", "latest")
                .load()
            )
    schema = StructType([StructField('id', IntegerType(), True), StructField('data', DecimalType(), True), StructField('date', StringType(), True)])
    sensor_df = (df.selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"),schema).alias("DATA"))
                .select("DATA.*")
                .withColumn("event_time", to_timestamp(col=col("date"),format="dd-MM-yyyy HH:mm:ss"))
                .drop("time_stamp")
                .where(col("id").isNotNull()))
    
    
    (sensor_df.writeStream 
      .format("console") 
      .outputMode("update") 
      .start() 
      .awaitTermination())
    
    