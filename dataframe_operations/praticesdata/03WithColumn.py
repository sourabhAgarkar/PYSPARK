from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('WithColumn').getOrCreate()

    data = [
        ('james', '', 'smith', '2023-07-11', 'm', 3000),
        ('michael', 'rose', '', '2023-07-09', 'm', 4000),
        ('Robert', ' ', 'williams', '2023-07-05', 'm',  4000),
        ('maria', 'anne', 'jones', '2023-07-04', 'f', 4000),
        ('jen', 'marry', 'brown', '2023-07-01', 'f', -1)
    ]

    df = spark.createDataFrame(data, ['firstname', 'middle', 'lastname', 'dob', 'gender', 'salary'])
    # df.printSchema()

    # change datatype using pyspark withColumn()
    sal = df.withColumn('salary', col('salary').cast('Integer'))
    # sal.printSchema()

    # update the value of an existing column
    #   df.withColumn('salary', col('salary')*10).show()

    # Create a column from an Existing
    #  df.withColumn('salary', col('salary') * 0.1).show()

    # Add a new Column using withColumn()
    #  df.withColumn('country', lit('INDIA')).show()

    # rename column name
    #   df.withColumnRenamed('gender', 'sex'), df.withColumnRenamed('salary', 'Payment').show()

    # Drop Column from pyspark dataframe
    #   df.drop('salary', 'dob', 'gender', 'firstname', 'middle', 'lastname',' ').show()

    spark.stop()
    #