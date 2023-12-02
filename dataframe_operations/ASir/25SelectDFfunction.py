from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, LongType
from pyspark.sql.functions import col

# Assignment No : 01
# Assignment description: Select data Frame Function
# Date:2023-02-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Data Frame with column" the application name
    spark = SparkSession.builder.master('local[*]').appName('Select data Frame Function').getOrCreate()

    input_data = [
        (('Rohit', 'Sharma'), 'India', 'IN'),
        (('kane', 'williamson'), 'NewZealand', 'NZ'),
        (('Glen', 'Maxwell'), 'Australia', 'AU'),
        (('Chris', 'Gayle'), 'WestIndies', 'WI'),
        (('Sachin', 'Undulate'), 'India', 'IN')
    ]

    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType()),
            StructField('lastname', StringType())
        ])),
        StructField('Country', StringType()),
        StructField('Short', StringType())
    ])

    df = spark.createDataFrame(input_data, schema=schema)
    df.printSchema()
    df.show()

    # select function
    df.select('name').show()
    df.select(df.name).show()
    df.select(df['name']).show()

    # nested select colum function
    df.select('name.firstName', 'name.lastName').show()
    df.select(df.name.firstName, df.name.lastName).show()
    df.select(df['name.firstName'], df['name.lastName']).show()
    df.select(df.columns[:]).show()

    # select multiple columns
    df.select('Country', 'Short').show()
    df.select(df.Country, df.Short).show()
    df.select(df['Country'], df['Short']).show()

    # all table columns
    df.select('*').show()

    # using function select column
    df.select(col('Country')).show()

    # shows the column name
    print(df.columns)

    # data types of column
    print(df.dtypes)

    # terminated spark session
    spark.stop()

    #