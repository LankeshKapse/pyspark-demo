from pyspark.sql import SparkSession
from pyspark.sql.functions import (explode,split,col,lower)


if __name__=="__main__":
    
    spark = SparkSession.Builder().appName(name='word_count_py').getOrCreate()
    df = spark.read.text("src/main/resources/word_count.txt").withColumnRenamed("value","lines")
    
    (df.select(explode(split(col("lines")," ")))
        .where("col != ''")
        .select(lower(col("col")).alias("words"))
        .groupBy("words").count()
        .orderBy(col("count").desc())
        .show(truncate=False))