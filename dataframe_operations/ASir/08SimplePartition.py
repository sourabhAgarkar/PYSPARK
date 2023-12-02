from pyspark.sql import SparkSession

# Assignment No:7
# Assignment description:Simple Partition
# Date:2023-20-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master('local[*]').appName("Simple Partition").getOrCreate()

    # Create rdd from input text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt")

    # get partitions
    print("Partition Size:", input_rdd.getNumPartitions())

    # terminated the spark session
    spark.stop()