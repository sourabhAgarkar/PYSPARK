from pyspark.sql import SparkSession

# Assignment No: 12
# Assignment Description:Accumulator:Write only variables and are shared variables
# Date:2023-21-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "BroadcastAccumulator" as the application name
    spark = SparkSession.builder.master("local[*]").appName("BroadcastAccumulator").getOrCreate()

    # creating rdd based existing collection
    input_rdd = spark.sparkContext.parallelize(range(0, 25))

    # Collect and print all elements from the input RDD
    print(input_rdd.collect())

    # initialize Accumulator
    accumulate_var = spark.sparkContext.accumulator(1000)

    # print Accumulator value
    print(f"Accumulator value:{accumulate_var.value}")

    # add value in Accumulator
    input_rdd.foreach(lambda x: accumulate_var.add(x))

    # print add value in Accumulator
    print(f"Accumulator after add value:{accumulate_var.value}")

    # terminated spark session
    spark.stop()