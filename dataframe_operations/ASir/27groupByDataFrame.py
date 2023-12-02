from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, avg, sum, count
# Assignment No:03
# Assignment Description : Group by in DataFrame
# Date:2023-05-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "GroupBY" the application name
    spark = SparkSession.builder.master('local[*]').appName('GroupBY').getOrCreate()

    # read top of the csv file
    input_df = spark.read.csv(r'C:\PYSPARK\04CSVFiles\crash_catalonia.csv', header=True, inferSchema=True)

    # show the csv file
    # input_df.show()

    # group by with max
    # input_df.groupBy('Day').max('Crashes').show()

    # group by with max value and using alias with import max,min,avg,sum functions
    input_df.groupBy('Day').agg(max('Crashes').alias('max_Crashes'),
                                min('Crashes').alias('min_Crashes'),
                                avg('Crashes').alias('avg_Crashes'),
                                sum('Crashes').alias('sum_Crashes'),
                                count('Crashes').alias('CountCrashes')).show()

    # spark session close
    spark.stop()

