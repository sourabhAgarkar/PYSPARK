from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import expr, col

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("When").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)

    df.show(df.count(), truncate=False)

    df.withColumn("Grade", when(df["state"] == 'NY', "NewYork")
                  .when(df["state"] == "CA", "Canada")
                  .otherwise(df["state"]))

    df.select("*", when(col("state") == "NY", "NewDelhi")
              .when(col("state") == "CA", "Carrot")
              .otherwise(col("state")) .alias("City"))

    df.withColumn("expr", expr("case when state = 'NY' then 'NewYork' "
                               " when state = 'CA' then 'CanaDa' else state"
                               " end"))

    df.select("state", expr("case when state = 'NY' then 'NewYork'"
                            " when state = 'CA' then 'Canada' else state end").alias("City"))

    df.createOrReplaceTempView("emp")

    spark.sql("select e.*,"
              " case"
              " when state = 'NY'"
              " then 'NewYork'"
              " when state = 'CA'"
              " then 'Canada'"
              " end state"
              " from emp e")

    df.withColumn("Text", when(col("age") >= 60, "Old")
                  .when(col("age") >= 50, "Younger")
                  .otherwise(col("age")))

    df.withColumn("Grade", when((col("age") >= 50) & (col("age") <= 60), "old")
                  .when((col("age") >= 40) & (col("age") <= 50),"Younger")
                  .when((col("age") >= 30) & (col("age") <= 40),"Adult")
                  .when((col("age") >= 30) & (col("age") <= 20), "yogo")
                  .otherwise(col("age")))

    df.withColumn("Grade", when((col("age") >= 20) & (col("age") <= 30), "Young")
                  .when((col("age") >= 30) & (col("age") <= 40), "Adult")
                  .when((col("age") >= 40) & (col("age") <= 50), "old")
                  .otherwise("oldGang")).show()


    spark.stop()