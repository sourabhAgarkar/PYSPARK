from pyspark.sql import SparkSession

# Assignment No:10
# Assignment description:Parallelize Partitions,2_Parallelize Repartition,3_Parallelize Coalesce
# Date:2023-20-06

if __name__ == '__main__':

    # initialization spark session
    spark = SparkSession.builder.master("local[*]").appName("Parallelize_Partitions,Repartition,Coalesce").getOrCreate()

    # Creating rdd based existing collection
    input_rdd = spark.sparkContext.parallelize(range(0, 25))

    # get Number of partitions Parallelize partitions
    print(f"Number of Partitions:{input_rdd.getNumPartitions()}")

    # write text file for parallelize
    input_rdd.saveAsTextFile(r"C:\PYSPARK\Output_store_ParallelizePartition\CParallelize01")
    

# 2nd scenario ParallelizeRepartition

    # Repartition input_rdd into 5 partitions
    repartition_rdd = input_rdd.repartition(5)

    # get Number of Partitions after repartition
    print(f"Number of Partitions after repartition:{repartition_rdd.getNumPartitions()}")

    # write text file for repartition
    repartition_rdd.saveAsTextFile(r"C:\PYSPARK\Output_store_ParallelizePartition\DRepartition02")

# 3rd scenario Coalesce repartition

    # coalesce
    coal_rdd = input_rdd.coalesce(5)

    # get Number of partitions after coalesce
    print(f"Number of partitions after coalesce:{coal_rdd.getNumPartitions()}")

    # write text file for coalesce
    coal_rdd.saveAsTextFile(r"C:\PYSPARK\Output_store_ParallelizePartition\ECoalesce03")

# terminated spark session
    spark.stop()