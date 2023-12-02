from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql.functions import col
if __name__ == '__main__':

    saurabh = SparkSession.builder.master('local[*]').appName('Rename').getOrCreate()

    data = [
        (('james','','smith'),'11-7-2023','M',3000),
        (('michael','rose',''),'9-7-2023','M',4000),
        (('Robert',' ', 'Williams'),'7-7-2023','M',4000),
        (('Maria','Anne','jones'),'5-7-2023','F',4000),
        (('jen','Mary','Brown'),'2-7-2023','F',-1)
    ]

    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType()),
            StructField('middle', StringType()),
            StructField('lastname', StringType())
        ])),
        StructField('date',StringType()),
        StructField('gender', StringType()),
        StructField('sal', IntegerType())
    ])

    df = saurabh.createDataFrame(data, schema=schema)
    #   df.printSchema()
    #   df.show()

    # to rename dataframe column name
    name = df.withColumnRenamed('date', 'DateOfBrith')
    #   name.printSchema()
    #   name.show()

    # to rename multiple columns
    df1 = df.withColumnRenamed('date', 'dateOfBrith')\
            .withColumnRenamed('gender', 'Sex')\
            .withColumnRenamed('sal', 'Salary')

    #   df1.printSchema()

    # Using select - to rename nested elements
    nested_rename = df.select(col('name.firstname').alias('Name'),
                              col('name.middle').alias('MiddleName'),
                              col('name.lastname').alias('LastName'))

    #   nested_rename.printSchema()

    # dataframe withColumn - to rename nested columns
    nested_rename1 = df.withColumn('Name', col('name.firstname'))\
                       .withColumn('middleName', col('name.middle'))\
                       .withColumn('LastName', col('name.lastname'))\
                       .drop('name')
    nested_rename1.printSchema()

    
    saurabh.stop()