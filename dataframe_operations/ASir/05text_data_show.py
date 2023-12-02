from pyspark.sql import SparkSession

# Assignment No :4
# Assignment description: text_data_show_input_file
# Date:2023-20-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master('local[*]').appName('text_data_show').getOrCreate()

    # create input rdd from text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\text.txt")

    # print input text file
    print(input_rdd.collect())

    # terminated spark session
    spark.stop()