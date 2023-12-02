from pyspark.sql import SparkSession
from pyspark.sql.functions import col, approx_count_distinct
from pyspark.sql.functions import avg
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import collect_set
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import count

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Aggregate Function").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)
    df.show()

    d1 = df.select(approx_count_distinct("state").alias("state"))

    d2 = df.select(approx_count_distinct(col="department").alias("department"))

    # avg(average) Aggregate Function

    df.select(avg(col("salary")).alias("salary"))

    df.select(avg("bonus"))

    df.select(avg(df["salary"]).alias("salary"))

    df.select(avg(df.salary).alias("Salary"))

    # collect_list Aggregate function
    df.select(collect_list("state").alias("state"))  # .show(truncate=False)

    # collect_set Aggregate function
    df.select(collect_set("state").alias("state"))  # .show(truncate=False)

    # countDistinct Aggregate Function
    df.select(countDistinct("department").alias("department"))

    # count function
    df.select(count(col("employee_name")).alias("emp"))

    df.select(count(df["department"]).alias("department"))

    df.select(count(df.state).alias("state"))

    df.select(count("salary").alias("salary"))

    spark.stop()