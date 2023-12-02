from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, lit, col, explode, trim, struct, lower, expr, sum, when, split, year, month, bround
import pyspark.sql.functions as F
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, lit, col

# Assignment:20
# Assignment description: Practise_Questions using csv file(ford_escort.csv)
# Date:2023-30-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "Schema in pyspark" as the Application Name
    spark = SparkSession.builder.master('local[*]').appName('Schema in pySpark').getOrCreate()

    # input on top of csv file
    input_rdd = spark.read.csv(r'C:\PYSPARK\Data\addresses.csv', header=True, inferSchema=True)

    # show csv file data
    input_rdd.show()

    # print schema
    input_rdd.printSchema()

    # change data type
    data_rdd = input_rdd.withColumn('zipcode', col('zipcode').cast('int'))

    # print Schema
    data_rdd.printSchema()

    # change data type
    data_rdd1 = input_rdd.withColumn('address', col('address').cast('int'))

    # print schema
    data_rdd1.printSchema()

    # terminate spark session
    spark.stop()