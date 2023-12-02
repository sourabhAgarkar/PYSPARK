from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import expr
from pyspark.sql.functions import substring
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Concat_ws").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)

    # df.show(n=2, truncate=False, vertical=True

    df.withColumn("Bio", concat_ws(" ", "employee_name", "department"))

    df.withColumn("Bio", concat_ws("_", "employee_name", "department", "state"))

    df.withColumn("Bio", expr("employee_name || department || state"))

    df.withColumn("Bio", expr("employee_name ||' '|| department ||'  '|| state"))

    df.select(col("employee_name")).withColumn("subData", substring("employee_name", 1, 4)).show()

    spark.stop()