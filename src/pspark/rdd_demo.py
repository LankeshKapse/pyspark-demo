from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = (SparkSession.builder
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .appName(name='rdd_demo').getOrCreate())

    df = spark.read.format("parquet").option("path", "../../docs/sensors-data/sensor_parquet").load()

    df.printSchema()
    print(df.rdd.take(5))
