from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.functions import min
from pyspark.sql.functions import avg
from pyspark.sql.functions import mean
from pyspark.sql.functions import sum
from pyspark.sql.functions import count


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("filter data with Aggregate function").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)
    df.show()
    df.filter(col("salary") >= 50000).select("salary").sort(df["salary"].desc())

    df.groupBy("department").agg(max(col("salary")).alias("salary")).filter(col("salary") >= 90000)\
        .orderBy(col("salary").desc())

    df.groupBy("state").agg(max("salary").alias("salary")).sort(col("salary").desc())

    df.createOrReplaceTempView("emp")
    spark.sql("select department, max(salary)salary from emp group by department having max(salary) >= 95000")

    df.groupBy(col("state")).agg(max("bonus").alias("max"),
                                 min("bonus").alias("min"),
                                 avg("bonus").alias("avg"),
                                 mean("bonus").alias("mean"),
                                 sum("bonus").alias("sum"),
                                 count("bonus").alias("count")).filter(col("state") == "NY")

    df.groupBy("department").agg(max("salary").alias("salary")).sort(col("salary").desc())





    spark.stop()