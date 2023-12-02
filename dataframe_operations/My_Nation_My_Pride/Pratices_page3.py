from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, lit, col, explode, trim, struct, lower, expr, sum, when, split, year, month, bround
import pyspark.sql.functions as F
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, lit, col

# Assignment:19
# Assignment description: Practise_Questions using csv file(ford_escort.csv)
# Date:2023-30-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "New_data" as the Application Name
    spark = SparkSession.builder.master('local[*]').appName('practice on ford_escort').getOrCreate()

    # input on top csv of file
    input_rdd = spark.read.csv(r"C:\PYSPARK\Data\ford_escort.csv", header=True, inferSchema=True)

    # show the csv file data
    input_rdd.show()
    print("without drop duplicate data:", input_rdd.count())

    # show table schema
    print(input_rdd.printSchema())

    # drop the duplicate records
    drop_rdd = input_rdd.dropDuplicates(['Year', 'Mileage', 'Price'])

    # show drops data in table
    drop_rdd.show()
    print("after drop duplicate data:", drop_rdd.count())

    # check in year column is null available or not
    input_rdd.where(col('Year').isNull()).show()

    # check in Mileage is null available or not
    input_rdd.where(col('Mileage').isNull()).show()

    # check in Price is null available or not
    input_rdd.where(col('Price').isNull()).show()

    # terminate spark session
    spark.stop()
