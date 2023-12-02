from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('Distinct,DropDuplicates').getOrCreate()

    data = [
        ('James', 'Sales', 3000),
        ('Michael', 'Sales', 4600),
        ('Robert', 'Sales', 4100),
        ('Maria','Finance', 3000),
        ('James','Sales',3000),
        ('Scott','Finance',3300),
        ('Jen','Finance',3900),
        ('Jeff','Marketing',3000),
        ('Kumar','Marketing',2000),
        ('Saif','Sales',4100)
    ]
    columns = ['firstname', 'department', 'salary']
    df = spark.createDataFrame(data=data, schema=columns)
    print(f'df:{df.count()}')
    df.show()

    # Get distinct rows (by comparing all columns)
    dfDistinct = df.distinct()
    print(f'Distinct:{dfDistinct.count()}')

    #
    drop_duplicate = df.dropDuplicates()
    print(f'drop_duplicates:{drop_duplicate}')

    # pyspark distinct of selected multiple columns
    columns = df.dropDuplicates(['department','salary'])
    print(f'dropDuplicates columns:{columns.count()}')

    spark.stop()