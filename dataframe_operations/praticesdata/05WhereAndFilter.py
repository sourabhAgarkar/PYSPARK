from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,ArrayType

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('filter and where').getOrCreate()

    data = [
        (('james','','smith'),['java','scala','c++'],'oh','m'),
        (('Anna','rose',''),['spark','java','c++'],'ny','f'),
        (('julia','','williams'),['csharp','vb'],'oh','f'),
        (('maria','anne','jones'),['csharp','vb'],'ny','m'),
        (('jen','mary','brown'),['csharp','vb'],'ny','m'),
        (('mike','mary','williams'),['python','vb'],'oh','m')
    ]

    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType()),
            StructField('middle', StringType()),
            StructField('lastname', StringType())
        ])),
        StructField('languages', ArrayType(StringType())),
        StructField('state', StringType()),
        StructField('gender', StringType())
    ])

    df = spark.createDataFrame(data=data, schema=schema)
    #   df.printSchema()
    df.show(truncate=False)

    # dataFrame filter() with column condition
    # Using equals condition

    # df.filter(df.state == 'oh').show()
    # df.where(df.state == 'oh').show()

    # not equals condition

    #   df.filter(df.state != 'oh').show()
    #   df.filter(~(df.state == 'oh')).show()

    # using SQL col() function
    from pyspark.sql.functions import col

    #   df.filter(col('state') == 'oh').show()
    #   df.filter(col('state') != 'oh').show()

    # Dataframe filter() with sql expression

    #   df.filter(df.gender == 'm').show()
    #   df.filter("gender == 'm' ").show()
    #   df.filter("gender == 'f' ").show()
    #   df.filter("state == 'ny' ").show()

    # pyspark filter with multiple Conditions

    #   df.filter((df.state == 'ny') | (df.gender == 'f')).show()
    #   df.filter((df.state == 'oh') & (df.gender == 'm')).show()

    #   filter Based on List values
        # filter is in list values

    #   df.filter(df.state.isin(['oh', 'ny'])).show()
    #   df.filter(df.gender.isin(['m', 'f'])).show()

        # filter not is in list values
    #   df.filter(~df.state.isin(['oh', 'ny'])).show()
    #   df.filter(df.state.isin(['oh', 'ny']) == False).show()

    #   filter based on starts with,ends with contains
      # using startswith

    #   df.filter(df.state.startswith('n')).show()
    #   df.filter(df.state.startswith('o')).show()

        #using endswith
    #   df.filter(df.state.endswith('h')).show()
    #   df.filter(df.state.endswith('y')).show()

    # pyspark filter and rlike

    #   df.filter(df.state.like('%ny%')).show()
    #   df.filter(df.state.like('%oh%')).show()

    #  ----------------------------------------------------------------------------------------------------------------

    # rlike -SQL RLIKE pattern (LIKE with Regex)
    # this check case insensitive

    df1 = spark.read.csv(r'C:\PYSPARK\Data\people.csv', header=True)
    #   df1.show()

    #   df1.filter(df1.middle.rlike('(?i)^*rose$')).show()

    #  ----------------------------------------------------------------------------------------------------------------

    # filtering on nested struct columns
    #    // Struct condition

    df.filter(df.name.lastname == 'williams').show(truncate=False)

    spark.stop()
    #