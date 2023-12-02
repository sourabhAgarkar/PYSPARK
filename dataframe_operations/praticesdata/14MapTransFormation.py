from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('MapTransFormation').getOrCreate()

    data = ["Project","Gutenberg’s","Alice’s","Adventures",
            "in","Wonderland","Project","Gutenberg’s","Adventures",
            "in","Wonderland","Project","Gutenberg’s"]

    # pyspark map() with RDD

    # rdd = spark.sparkContext.parallelize(data)
    # collect = rdd.map(lambda x: (x, 1))

    # for x in collect.collect():
    #     print(x)

    #   ------------------------------------------------------------------------------------

    # pyspark map() with DataFrame

    dataDF = [
        ('James', 'Smith', 'M', 30),
        ('Anna', 'Rose', 'F', 41),
        ('Robert', 'Williams', 'M', 62)
    ]
    df = spark.createDataFrame(dataDF, ['firstName', 'LastName', 'Gender', 'Age'])

    # Referring columns by index
    rdd1 = df.rdd.map(lambda x: (x[0], x[1], x[2], x[3]))
    df1 = rdd1.toDF(['name', 'last', 'gender', 'age'])
    #df1.show()

    # Referring columns Names
    rdd2 = df.rdd.map(lambda x: (x['firstName'], x['LastName'], x['Gender'], x['Age']))
    df2 = rdd2.toDF(['name', 'sirname', 'sex', 'age'])
    #df2.show()

    # Referring column Names
    rdd3 = df.rdd.map(lambda x: (x.firstName, x.LastName, x.Gender, x.Age))
    df3 = rdd3.toDF(['col1', 'col2', 'col3', 'col4'])
    df3.show()
    spark.stop()