from pyspark.sql import SparkSession

# Assignment No:07
# Assignment Description : Window Functions:1)dense_rank 2)rank 3)row_number 4)lag 5)lead
# Date:2023-07-07

if __name__ == '__main__':

      # create SparkSession with local[*] as the master and "Window Functions" the application name
      spark = SparkSession.builder.master('local[*]').appName('Window Functions').getOrCreate()

      print(spark)

      # spark session close
      spark.stop()