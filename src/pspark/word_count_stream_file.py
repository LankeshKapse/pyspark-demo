from pyspark.sql import SparkSession
from pyspark.sql.functions import (explode,split,col,lower)

if __name__=="__main__":
    spark = (SparkSession.Builder()
             .config("spark.sql.shuffle.partitions", "10")
             .appName(name="read_stream_file_py").getOrCreate())
    
    df =(spark.readStream
     .text("src/main/resources/stream/text")
     .withColumnRenamed("value","lines")
     .select(explode(split(col("lines")," ")))
        .where("col != ''")
        .select(lower(col("col")).alias("words"))
        .groupBy("words").count()
        .orderBy(col("count").desc())
     )
    
    (df
     .writeStream.format("console")
     .outputMode("complete")
     .start()
     .awaitTermination())