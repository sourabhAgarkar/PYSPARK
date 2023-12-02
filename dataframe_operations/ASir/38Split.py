from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import col

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Split").getOrCreate()

    df = spark.read.csv(path=r"C:\PYSPARK\04praticesdataInput\03Pivot.csv", header=True, inferSchema=True)
    df.show(n=2)

    df.select(split(col("product"), " ", limit=-1).alias("product"))

    df.createOrReplaceTempView("emp")
    spark.sql("select split(product, ',') product from emp").show()

    spark.stop()