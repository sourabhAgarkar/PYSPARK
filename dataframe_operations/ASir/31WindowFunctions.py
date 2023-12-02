from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank, rank, row_number, lag, lead

# Assignment No:07
# Assignment Description : Window Functions:1)dense_rank 2)rank 3)row_number 4)lag 5)lead
# Date:2023-07-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Window Functions" the application name
    spark = SparkSession.builder.master('local[*]').appName('Window Functions').getOrCreate()

    # create df on top of the csv file
    df = spark.read.csv(r'C:\PYSPARK\04praticesdataInput\03Pivot.csv', header=True, inferSchema=True)
    df.show()

    # Calculating dense rank based on 'country' and 'amount' in descending order
    df.withColumn('dense_rank', dense_rank().over(Window.partitionBy('country').orderBy(col('amount').desc())))

    # Calculating rank based on 'country' and 'amount' in descending order
    df.withColumn('rank', rank().over(Window.partitionBy('country').orderBy(col('amount').desc())))

    # Calculating row_number based on 'country' and 'amount' in descending order
    df.withColumn('row_number', row_number().over(Window.partitionBy('country').orderBy(col('amount').desc())))

    # Calculating leg based on 'country' and 'amount' in descending order
    df.withColumn('lag', lag('amount', 1).over(Window.partitionBy('country').orderBy(col('amount').desc())))

    # Calculating lead based on 'country' and 'amount' in descending order
    df.withColumn('lead', lead('amount', 2).over(Window.partitionBy('country').orderBy(col('amount').desc())))

    # maximum salary
    #df.withColumn('max_salary', max('Taxes').over(Window.partitionBy('Beds').orderBy(col('Taxes').desc())))

    # spark session terminated
    spark.stop()
    #

    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank, col, rank, row_number

    if __name__ == '__main__':
        spark = SparkSession.builder.master('local[*]').appName('Windows functions').getOrCreate()

        df = spark.read.csv(r'C:\PYSPARK\04praticesdataInput\03Pivot.csv', header=True, inferSchema=True)

        # df.withColumn('dense_rank', dense_rank().over(Window.partitionBy('country').orderBy(col('amount').desc()))).show()
        # df.withColumn('dense_rank', dense_rank().over(Window.orderBy(col('amount').desc()))).show()

        window = df.withColumn('rank', rank().over(Window.partitionBy('country').orderBy(col('amount').desc()))) \
            .withColumn('dense_rank', dense_rank().over(Window.partitionBy('country').orderBy(col('amount').desc()))) \
            .withColumn('row_number', row_number().over(Window.partitionBy('country').orderBy(col('amount').desc())))

        window.show(truncate=False)

        Window = df.withColumn('rank', rank().over(Window.orderBy(col('amount').desc()))) \
            .withColumn('dense_rank', dense_rank().over(Window.orderBy(col('amount').desc()))) \
            .withColumn('row_number', row_number().over(Window.orderBy(col('amount').desc())))

        Window.show()

        spark.stop()