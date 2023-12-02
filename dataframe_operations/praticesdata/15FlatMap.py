from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('FlatMap transformation').getOrCreate()

    data = [
        "Project Gutenberg's ",
        "Alice's Adventures in Wonderland ",
        "Project Gutenberg's ",
        "Adventures in Wonderland",
        "Project Gutenberg's "
    ]
    rdd = spark.sparkContext.parallelize(data)
    rdd1 = rdd.flatMap(lambda x: x.split(' '))
    print(rdd1.collect())

    # OR

    for element in rdd1.collect():
        print(element)

    spark.stop()