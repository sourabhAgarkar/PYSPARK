from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, avg, collect_list, collect_set, count, count_distinct,\
    countDistinct,first, last, max, max_by, mean, min, min_by, sum, sum_distinct, sumDistinct

# Assignment No:06
# Assignment Description : Aggregate Functions
# Date:2023-07-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Aggregate Functions" the application name
    spark = SparkSession.builder.master('local[*]').appName('Aggregate Functions').getOrCreate()

    # create df on top of the csv file
    df = spark.read.csv(r'C:\PYSPARK\04CSVFiles\cities.csv', header=True, inferSchema=True)

    # show dataframe
    df.show(4)

    # approx_count_distinct: returns a new column for approximate distinct count of column col
    df.select(approx_count_distinct('LonD').alias('distinct_values')).show()

    # avg(col): returns the average of the values in a group
    df.select(avg('LonD')).show()

    # collect_list(col): returns a list of objects with duplicates
    df.select(collect_list('LonD')).show(truncate=False)

    # collect_set(col): returns a set of objects with duplicate elements eliminated
    df.select(collect_set('LonD')).show(truncate=False)

    # count(col):returns the number of items in a group
    df.select(count('City')).show()

    # count_distinct(col,*cols):returns a new column for distinct count of col or cols
    df.select(count_distinct('City')).show()

    # countDistinct(col,*cols):Returns a new column for distinct count of col or cols
    df.select(countDistinct('City')).show()

    # first(col[, ignorenulls]): returns the first value in a group
    df.select(first('City', ignorenulls=True)).show()

    # last(col,[ ignorenulls]): returns the last value in a group
    df.select(last('City', ignorenulls=True)).show()

    # max(col): returns the maximum value of the expression in a group.
    df.select(max('LonD')).show()

    # max_by(col,ord) Returns the value associated with the maximum value or ord
    df.select(max_by('LonD', 'City')).show()

    # mean(col) Returns the average of the values in a group
    df.select(mean('LonD')).show()

    # min(col) Returns the minimum value of the expression in a group
    df.select(min('LonD')).show()

    # min_by(col,ord) Returns the value associated with the minimum value of ord
    df.select(min_by('LonD', 'City')).show()

    # sum(col) : Returns the sum of all values in the expression
    df.select(sum('LonD')).show()

    # sum_distinct(col) returns the sum of distinct values in the expression
    df.select(sum_distinct('LonD')).show()

    # sumDistinct(col) returns the sum of distinct values in the expression
    df.select(sumDistinct('LonD')).show()

    # spark session terminated
    spark.stop()