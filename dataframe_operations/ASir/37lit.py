from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("lit function").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)
    df.show(n=4)

    df.select(lit("1").alias("staticCol"))

    df.withColumn("staticDuplicate", lit("saurabh"))

    df.select(lit("Agar").alias("SirName"))

    df.withColumn("SirName", lit("Agar_kar")).show()

    spark.stop()