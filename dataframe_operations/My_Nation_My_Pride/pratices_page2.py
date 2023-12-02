from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, lit, col, explode, trim, struct, lower, expr, sum, when, split, year, month, bround
import pyspark.sql.functions as F
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, lit, col

# Assignment:19
# Assignment description: Practise_Questions using csv file(homes.csv)
# Date:2023-30-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "New_data" as the Application Name
    spark = SparkSession.builder.master("local[*]").appName("New_Data").getOrCreate()

    # input on top csv of file
    input_rdd = spark.read.csv(r"C:\PYSPARK\Data\homes.csv", header=True)

    # show the csv file data
    input_rdd.show(5)

    # select some columns
    select_rdd = input_rdd.select('Rooms', 'Beds', 'Baths')

    # show the selected columns
    select_rdd.show(5)

    # distinct room
    distinct_rdd = input_rdd.select('Rooms').distinct()

    # show the distinct rooms
    distinct_rdd.show()

    # count the total distinct rooms
    print("Distinct Rooms:", input_rdd.select('Rooms').distinct().count())

    # shows the columns sell distinct data, only first 5 records
    input_rdd.select("Sell").distinct().show(5)

    # shows the columns Beds distinct data,only first 5 records
    input_rdd.select("Beds").distinct().show()

    # terminated spark session
    spark.stop()