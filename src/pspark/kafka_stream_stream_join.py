from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
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

    status_df = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "sensor.status")
                 .option("startingOffsets", "latest")
                 .load()
                 )
    schema = StructType([StructField('id', IntegerType(), True), StructField('data', DecimalType(10, 2), True),
                         StructField('date', StringType(), True)])
    sensor_df = (df.selectExpr("CAST(value AS STRING)")
                 .select(from_json(col("value"), schema).alias("DATA"))
                 .select("DATA.*")
                 .withColumn("event_time", to_timestamp(col=col("date"), format="dd-MM-yyyy HH:mm:ss"))
                 .drop("date")
                 .where(col("id").isNotNull())
                 .withWatermark("event_time", "2 seconds")
                 )

    schema_status = StructType([StructField('sensor_id', IntegerType(), True),
                                StructField('sensor_loc', StringType(), True),
                                StructField('control', StringType(), True),
                                StructField('counter', IntegerType(), True),
                                StructField('date', StringType(), True)
                                ])
    sensor_status_df = (status_df.selectExpr("CAST(value AS STRING)")
                        .select(from_json(col("value"), schema_status).alias("DATA"))
                        .select("DATA.*")
                        .withColumn("status_event_time", to_timestamp(col=col("date"), format="dd-MM-yyyy HH:mm:ss"))
                        .drop("date")
                        .where(col("sensor_id").isNotNull())
                        .withWatermark("status_event_time", "2 seconds")
                        )

    join_df = (sensor_df.join(sensor_status_df,
                              expr(
                                  "id = sensor_id and status_event_time between event_time and event_time + interval "
                                  "2 seconds"))

               )

    query_1 = (sensor_df.writeStream
               .queryName("sensor_data")
               .format("console")
               .outputMode("update")
               .option("checkpointLocation", "../../checkpoint-dir-1")
               .start())

    query_2 = (sensor_status_df.writeStream
               .queryName("sensor_status")
               .format("console")
               .outputMode("update")
               .option("checkpointLocation", "../../checkpoint-dir-2")
               .start())

    query_3 = (join_df.writeStream
               .queryName("join_df")
               .format("console")
               .outputMode("append")
               .option("checkpointLocation", "../../checkpoint-dir-3")
               .start())

    spark.streams.awaitAnyTermination()
