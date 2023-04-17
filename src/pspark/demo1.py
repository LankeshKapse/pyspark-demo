from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .config("spark.sql.shuffle.partitions", "2")
             .appName(name="demo").getOrCreate())

    spark.range(100000)\
        .where("id > 500")\
        .selectExpr("id as value", "(id *id ) as sqr", "(id * id * id) as cube")\
        .show()

    # input("Enter to close program")
