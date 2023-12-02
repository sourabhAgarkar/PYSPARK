from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("regexp_replace").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\03Pivot.csv", header=True, inferSchema=True)
    df.show()

    df.select(regexp_replace(col("amount"), '1', '5').alias("amount"))

    df.select(regexp_replace(df.amount, '4', 'S').alias("amount"))

    df.select(regexp_replace(df["amount"], '2', 'Saurabh').alias("amount"))

    df.select(regexp_replace("amount", '15', "Agar_kar").alias("Amount"))

    # withColumn

    df.withColumn("new", regexp_replace(col("country"), "usa", "United State of America"))  # .show(truncate=False)

    df.withColumn("Country", regexp_replace(col("country"), "usa", "United State of America")) \
        .withColumn("Country", regexp_replace(col("country"), "china", "CHINA")) \
        .withColumn("Country", regexp_replace(col("country"), "canada", "CANADA")).show(truncate=False)

    df.select(regexp_replace(regexp_replace(col("state"), "NY", "NewYork"), "CA", "Canada").alias("state"))

    spark.stop()