from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('collect data').getOrCreate()

    dept = [
        ('finance',10),
        ('marketing',20),
        ('sales',30),
        ('it',40)
    ]

    schema = StructType([
        StructField('dept', StringType()),
        StructField('id',IntegerType())
    ])

    df = spark.createDataFrame(dept, schema=schema)
    # df.show(truncate=False)

    # collect() retrieve the data
    # print(df.collect())

    # returns value of first row
    print(df.collect()[0])

    # returns value of all rows
    print(df.collect()[:])

    dept = df.select('dept').collect()
    print(dept)

    id = df.select('id').collect()
    print(id)

    spark.stop()