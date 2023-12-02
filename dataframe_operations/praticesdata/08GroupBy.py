from pyspark.sql import SparkSession
from pyspark.sql.functions import max, sum, count, mean, min, avg

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('groupBy/groupby').getOrCreate()

    df = spark.read.csv(r'C:\PYSPARK\04praticesdataInput\02groupBy',header=True)
    df.show()

    # groupBy on dataframe columns
# max
    df.groupBy('state','department').agg(sum('salary')).show()
# sum
    #   df.groupBy('department').agg(sum('salary')).show()
# count
    #   df.groupBy('department').agg(count('salary')).show()
# mean
    #   df.groupBy('state').agg(mean('bonus')).show()
# min
    #   df.groupBy('employee_name').agg(min('age')).show()
# avg
    #   df.groupBy('state').agg(avg('age')).show()

# Using Multiple columns
# GroupBy on Multiple Columns
    #   df.groupBy('department', 'state').agg(sum('salary')).show()
    #   df.groupBy('state', 'age').agg(max('salary')).show()

# Running more aggregates at a time
    store = df.groupBy('department').agg(sum('salary'),
                                 mean('salary'),
                                 max('salary'),
                                 min('salary'),
                                 avg('salary'))
    # store.show()
# Using filter on aggregate data


    spark.stop()