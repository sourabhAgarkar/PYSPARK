from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('select function').getOrCreate()

    data = [
        ('james', 'smith', 'usa', 'ca'),
        ('michael', 'rose', 'usa', 'na'),
        ('robert', 'williams', 'usa', 'ca'),
        ('maria', 'jones', 'usa', 'fl')
    ]

    df = spark.createDataFrame(data, ['firstname', 'lastname', 'country', 'state'])

    # df.select(df.columns).show()

    # select nested struct columns from pyspark

    data1 = [
        (('james',' ','simth'),'oh','m'),
        (('anna','rose',' '),'ny','f'),
        (('julia',' ','williams'),'oh','f'),
        (('maria','anne','jones'),'ny','m'),
        (('mike','mary','williams'),'oh','m')
    ]

    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType()),
            StructField('middle', StringType()),
            StructField('lastname', StringType())
        ])),
        StructField('city', StringType()),
        StructField('gender', StringType())
    ])

    df1 = spark.createDataFrame(data1, schema=schema)
    # df1.show()
    # df1.printSchema()

    # df1.select('name').show(truncate=False)
    # df1.select(df1.name).show()
    df1.select(df1.name.firstname).show()


    spark.stop()