from pyspark.sql import SparkSession

# Assignment No:2
# Assignment description: Repartition,coalesce
# Date: 2023-19-06


if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("partition_coalesce").getOrCreate()

#
    # create rdd from input text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt")

    # get By default Partition
    print(f"text file partition:{input_rdd.getNumPartitions()}")

#
    # create rdd from input text file with provide partition number
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt", 4)

    # get Number of partition
    print(f"text file partition with partition provide:{input_rdd.getNumPartitions()}")

#
    # create rdd from input text file with (parallelize)
    input_rdd = spark.sparkContext.parallelize(r"C:\PYSPARK\Data\input_data.txt", 6)

    # get partition
    print(f"text file partition with partition provide:{input_rdd.getNumPartitions()}")

#
    # creating rdd based on existing collection (partition by default)
    input_rdd = spark.sparkContext.parallelize(range(1, 25))

    # get number of partition
    print(f"Partition Size parallelize data:{input_rdd.getNumPartitions()}")

#
    # creating rdd based on existing collection
    input_rdd = spark.sparkContext.parallelize(range(0, 25))

    # get number of partition
    print(f"Partition size parallelize data:{input_rdd.getNumPartitions()}")

    # write rdd to textfile
    input_rdd.saveAsTextFile(r"C:\PYSPARK\output_data\01SimplePartition")

    # 2nd scenario
    # using Repartition()

#
    # creating rdd based existing collection
    input_rdd = spark.sparkContext.parallelize(range(0, 25))

    # Repartition
    repartition_rdd = input_rdd.repartition(4)

    # get number of partition
    print(f"partition size after Repartition:{repartition_rdd.getNumPartitions()}")

    # write rdd to textfile
    repartition_rdd.saveAsTextFile(r"C:\PYSPARK\output_data\02repartition")

    # 3rd scenario
    # using Coalesce():

#
    # creating rdd based existing collection
    input_rdd = spark.sparkContext.parallelize(range(1, 25))

    # coalesce
    coal_rdd = input_rdd.coalesce(4)

    # get Number of partition
    print(f"partition size after Coalesce{coal_rdd.getNumPartitions()}")

    # Write rdd to textfile
    coal_rdd.saveAsTextFile(r"C:\PYSPARK\output_data\03coalesce")

    # spark session terminate
    spark.stop()
    #