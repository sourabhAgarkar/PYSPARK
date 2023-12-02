from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("concat_ws").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\03Pivot.csv", header=True, inferSchema=True)
    df.printSchema()

    # select

    df.select(concat_ws(",", col("product"), col("country")).alias("product"))

    df.select(concat_ws("__", df["product"], df["country"]).alias("product"))

    df.select(concat_ws(" ", "product", "country").alias("product"))

    df.select(concat_ws("<>", df.product, df.country).alias("productCountry"))

    df.select(concat_ws(">>", df.product, df.amount, df.country).alias("product"))  # .show(truncate=False)

    df.select(concat_ws("=", df.product, df.amount).alias("product"))

    #  withColumn

    df.withColumn("price", concat_ws("_", col("product"), col("amount"))).select(col("price"))

    df.withColumn("Amount", concat_ws(":", df["product"], df["amount"])).select("Amount")

    df.withColumn("store", concat_ws(">", df.product, df.amount)).select(col("store")).show()

    spark.stop()