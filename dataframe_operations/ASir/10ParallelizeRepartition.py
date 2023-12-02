from pyspark.sql import SparkSession

# Assignment No:09
# Assignment description:Parallelize Partitions,Parallelize Repartition
# Date:2023-20-06

if __name__ == '__main__':

    # initialization spark session
    spark = SparkSession.builder.master("local[*]").appName("Parallelize Repartition").getOrCreate()

    # creating rdd based existing  collection
    input_rdd = spark.sparkContext.parallelize(range(1, 25))

    # get number of partitions
    print(f"Get Number Partition:{input_rdd.getNumPartitions()}")

    # write text file
    input_rdd.saveAsTextFile(r"C:\PYSPARK\Output_store_ParallelizePartition\AParallelize")


# Repartition 2nd scenario:

    # repartition
    repartition_rdd = input_rdd.repartition(4)

    # get number of partition after repartition
    print(f"Get Number of partition after repartition:{repartition_rdd.getNumPartitions()}")

    # write text file for repartition
    repartition_rdd.saveAsTextFile(r"C:\PYSPARK\Output_store_ParallelizePartition\BParallelizeRepartition")


# terminated spark session
    spark.stop()