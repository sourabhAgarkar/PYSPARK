from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, sum
from pyspark.sql.window import Window


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Aggregation").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)

    df.select("department").withColumn("Avg", avg("salary").over(Window.partitionBy("department")))\
        .select("department").withColumn("min", min("salary").over(Window.partitionBy("department")))\
        .select("department").withColumn("max", max("salary").over(Window.partitionBy("department")))\
        .select("department").withColumn("sum", sum("salary").over(Window.partitionBy("department")))

    spark.stop()