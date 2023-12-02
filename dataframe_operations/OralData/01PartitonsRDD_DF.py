from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('RDD Partition').getOrCreate()

    rdd = spark.sparkContext.parallelize(range(25))
    # print(rdd.collect())
    # print(f"Number of Partitions:{rdd.getNumPartitions()}")

    rdd1 = spark.sparkContext.parallelize(range(25), 6)
    # print(rdd1.collect())
    # print(f"Get Number Or Partitions:{rdd1.getNumPartitions()}")

    rdd_text = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt", 4)
    # print(f"Get Number of partitions text file:{rdd_text.getNumPartitions()}")
    # rdd_text.saveAsTextFile(r"C:\PYSPARK\dataframe_operations\OralDataOutput\text.txt")

    rdd_csv = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt")
    # print(f"Get Number of partitions text file:{rdd_csv.collect()}")
    # rdd_csv.saveAsTextFile(r"C:\PYSPARK\dataframe_operations\OralDataOutput\input.txt")
    # repartitions = rdd_csv.repartition(2)
    # repartitions.saveAsTextFile(r'C:\PYSPARK\dataframe_operations\OralDataOutput\repartition.txt')
    # coalesce = rdd_csv.coalesce(3)
    # coalesce.saveAsTextFile(r'C:\PYSPARK\dataframe_operations\OralDataOutput\coalesce.txt')






    spark.stop()