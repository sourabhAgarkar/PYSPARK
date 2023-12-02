from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,sum,to_date
# Assignment No:01
# Assignment Description : InterView Question
# Date:2023-08-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Window Functions" the application name
    spark = SparkSession.builder.master('local[*]').appName('Window Functions').getOrCreate()

    # create df on top of the csv file
    df = spark.read.csv(r'C:\PYSPARK\Data\data1.csv', header=True, inferSchema=True)

    df.withColumn('trans', sum('tran_amount').over(Window.partitionBy(to_date('trans_timestamp')).orderBy(col('trans_id').desc()))).show()

    df.withColumn('trans', sum('tran_amount').over(Window.partitionBy('cust_id', to_date('trans_timestamp')) \
                                                   .orderBy(col('trans_id').desc())))\
        .select('cust_id', 'trans_timestamp', 'trans').show()

    # spark session terminated
    spark.stop()
    #