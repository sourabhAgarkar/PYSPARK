from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('RDD Partition').getOrCreate()

    rdd = spark.sparkContext.parallelize(range(25))
    # print(rdd.collect())
    print(f"Number of Partitions:{rdd.getNumPartitions()}")

    rdd1 = spark.sparkContext.parallelize(range(25), 6)
    # print(rdd1.collect())
    print(f"Get Number Or Partitions:{rdd1.getNumPartitions()}")

    rdd_text = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt", 4)
    print(f"Get Number of partitions text file:{rdd_text.getNumPartitions()}")
    spark.stop()