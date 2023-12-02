from pyspark.sql import SparkSession

# Assignment No:08
# Assignment description:Parallelize Partition simple type
# Date:2023-20-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("Parallelize Partition").getOrCreate()

    # Creating rdd based on existing collection
    input_rdd = spark.sparkContext.parallelize(range(0, 25))

    # get number of partition
    print(f"Partition Size:{input_rdd.getNumPartitions()}")

    # write to textFile
    input_rdd.saveAsTextFile(r"C:\PYSPARK\Output_store_ParallelizePartition\AAParallelizePartition1")


# terminated spark session
    spark.stop()