from pyspark.sql import SparkSession

# Assignment:3
# Assignment description: SparkSession Start
# Date:2023-20-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master('local[*]').appName('first_program').getOrCreate()

    # print spark Session
    print(spark)

    # terminated spark session
    spark.stop()