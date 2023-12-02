from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("when").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True)
    df.show(df.count())

    df.select(when(col("state") == "NY", "NewYork")
              .when(col("state") == "CA", "Canada")
              .otherwise(col("state")).alias("state"))

    df.select(when(df["salary"] >= 9000, "A")
              .otherwise(df["salary"]))

    df.select(when(df["salary"] >= 90000, "A")
              .when(df["salary"] >= 80000, "B")
              .when(df["salary"] >= 70000, "C").alias("salary"))

    df.withColumn("Grade", when(df["salary"] >= 90000, "A")
                  .when(df["salary"] >= 80000, "B")
                  .when(df["salary"] >= 70000, "C"))

    df.select(expr("case when state = 'NY' then 'NewYork' when state = 'CA' then 'Canada' end ").alias("state"))

    df.withColumn("department", expr("case when department = 'Sales' then 'Sale'" +
                                     "when department = 'Finance' then 'fina' end"))

    df.select("*", expr("case when department = 'Finance' then 'fina'" +
                        "when department = 'Marketing' then 'mark'" +
                        "when department = 'Sales' then 'sale' end ").alias("UpdateDepart"))

    df.createOrReplaceTempView("emp")
    spark.sql("select e.* ,  case when state = 'NY' then 'NewYork' when state = 'CA' then 'Canada' end from emp e")\
        .alias("state")

    df.select("*", when(df["state"] == "NY", "NewYork")
              .when(df["state"] == "CA", "Canada")
              .otherwise(df["state"]).alias("state"))

    df.select("*")

    spark.stop()