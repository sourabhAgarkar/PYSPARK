from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, lit, col, explode, trim, struct, lower, expr, sum, when, split, year, month, bround
import pyspark.sql.functions as F
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, lit, col

# Assignment:19
# Assignment description: Practise_Questions
# Date:2023-29-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "DF With header,inferSchema" as the Application Name
    spark = SparkSession.builder.master("local[*]").appName("read csv file").getOrCreate()

    # input on top of file
    input_rdd = spark.read.csv(r"C:\PYSPARK\02dataframeIPcsv\addresses.csv", header=True)

    # Display the contents of the input RDD
    input_rdd.show()

    # create a new column with same value
    new_col_rdd = input_rdd.withColumn("new_postcode", col("postcode"))

    # show the new column added
    #new_col_rdd.show()

    # remove the decimal point value
    remove_rdd = input_rdd.withColumn("postcode", bround(F.bround("postcode")))

    # data show
    #remove_rdd.show()

    # add new column
    add_new_rdd = input_rdd.withColumn('Country', lit('India'))

    # show new column added into table
    #add_new_rdd.show()

    # delete a column
    delete_rdd = input_rdd.drop('post')

    # remove the new_postcode column
    #delete_rdd.show()

    # filter the data using where
    filter_rdd = input_rdd.where(col('city') == 'NJ')

    # filter data show
    #filter_rdd.show()

    # filter the data using filter
    filter1_rdd = input_rdd.filter(col('city') == 'NJ')

    # Show the filter data
    #filter1_rdd.show()

    # renamed the column
    rename_rdd = input_rdd.withColumnRenamed('postcode', 'post')\
                          .withColumnRenamed('last_name', 'l_name')\
                          .withColumnRenamed('first_name', 'f_name')

    # show rename column data
    #rename_rdd.show()

    # filter the multiple records using where
    filter_mul_rdd = input_rdd.where(
        (col('city') == 'NJ') |
        (col('city') == 'SD') |
        (col('city') == 'CO')
    )

    # show multiple filter records
    #filter_mul_rdd.show()

    # filter the data using isin
    isin_rdd = input_rdd.where(col('city').isin('NJ', 'PA', 'SD'))

    # show filter data using isin
    #isin_rdd.show()

    # filter don't show the select records
    is_in_rdd = input_rdd.where(~col('city').isin('NJ', 'PA', 'SD'))

    # show the filter data
    is_in_rdd.show()

    # terminate spark session
    spark.stop()
    #