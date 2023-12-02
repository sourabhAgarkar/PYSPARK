from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("substring").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)

    # substring

    df.withColumn("substring", substring(col("department"), 1, 3)).select(col("substring"))

    df.withColumn("substring", substring(df["department"], 1, 4)).select("substring")

    df.withColumn("substring", substring(df.department, 3, 5)).select("substring")

    df.withColumn("substring", substring("department", 2, 5)).select("substring")

    # substring Select
    df.select(substring(col("department"), 1, 3).alias("department"))

    df.select(substring(df["department"], 1, 3).alias("dep"))

    df.select(substring("department", 1, 4).alias("dep"))

    df.select(substring(df.department, 1, 3).alias("dep"))

    df.select(substring(col("department"), 2, 3).alias("dep"))

    spark.stop()