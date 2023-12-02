from pyspark.sql import SparkSession

# Assignment No:5
# Assignment description: take input file,split data
# Date:2023-20-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master('local[*]').appName('Split_data').getOrCreate()

    # create rdd from input text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\text.txt")

    # print the text data
    # print(input_rdd.collect())

    # split input rdd content ,to get each element  from rdd
    split_rdd = input_rdd.flatMap(lambda x:x.split(" "))

    # split the data
    print(split_rdd.collect())

    # terminated spark session
    spark.stop()