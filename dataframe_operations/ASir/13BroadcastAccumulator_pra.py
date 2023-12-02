from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Pratices").getOrCreate()

    input_rdd = spark.sparkContext.parallelize(range(0, 25))

    accumulate_var = spark.sparkContext.accumulator(100)

    print(f"Accumulator Value: {accumulate_var.value}")

    input_rdd.foreach(lambda x: accumulate_var.add(x))

    print(accumulate_var.value)

    spark.stop()