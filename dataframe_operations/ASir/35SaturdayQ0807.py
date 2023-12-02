from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,count,max,lag,month,last
from pyspark.sql.window import Window

# Assignment No:07
# Assignment Description : Question saturday 08-07-23
# Date:2023-08-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Window Functions" the application name
    spark = SparkSession.builder.master('local[*]').appName('Window Functions').getOrCreate()

    # create df on top of the csv file
    df = spark.read.csv(r'C:\PYSPARK\Data\df.csv.csv', header=True, inferSchema=True)
    df.show()

    # 1.calculate how many trans done in day. (trans_date, count)

    # convert_df = df.withColumn('datetime', to_date(col('trans_datetime')))
    # convert_df.groupBy('datetime').agg(count('*').alias('count')).show()

    # 2.cal max trans done in each city  on daily basis.(city, trans_date, max_amt)
    # convert_df.groupBy('datetime', 'trans_city').agg(max('credit_amt').alias('max_credit')).show()

    # 3.for each customer calculate diff bet trans time(cust_id,date_diff(hours)).
    # df.withColumn('prev_datetime', lag('trans_datetime').over(Window.partitionBy('cust_id').orderBy('trans_datetime')))\
    #     .show()

    # 4.provide cust_id, cust_name who is in debt at the end of the month.
    df.withColumn('credit_amt', df['credit_amt'].cast('float')).printSchema()
    df.withColumn('debit_amt', df['debit_amt'].cast('float')).printSchema()

    end_month = '2023-07-30'
    month_df = df.filter(month(to_date('trans_datetime')) == last(end_month))
    month_df.show()

    # spark session terminated
    spark.stop()

    #