from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("expr").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)
    df.show(n=4)

    df.withColumn("address", expr("employee_name || department")).select(col("address"))
    df.withColumn("empName_dep", expr("employee_name || '_' || department")).select(col("empName_dep"))

    df.select(expr("employee_name || department").alias("empName_dep"))
    df.select(expr("employee_name || '_' || department").alias("name"))

    df.select(when(col("state") == "NY", "NewYork")
              .otherwise(col("state")).alias("state"))

    df.select(when(col("state") == "NY", "NewYork")
              .when(col("state") == "CA", "Canada")
              .otherwise(col("state")) .alias("State"))

    df.withColumn("state", when(col("state") == "NY", "NewYork")
                  .otherwise(col("state")))

    df.withColumn("state", when(col("state") == "NY", "NewYork")
                  .when(col("state") == "CA", "Canada")
                  .otherwise(col("state")))

    df.withColumn("state", expr("CASE WHEN state = 'NY' THEN 'NewYork' ELSE state END"))

    df.select(expr("case when state = 'NY' then 'NewYork' when state = 'CA' then 'canada' end").alias("State"))

    df.withColumn("state", expr("case when state = 'NY' then 'NewYork' when state = 'CA' then 'Canada' end"))

    df.withColumn("department", expr("case when department = 'Sales' then 'SalesDepartment' when department = 'Finance' then 'FinaceDepart'  end "))

    spark.stop()