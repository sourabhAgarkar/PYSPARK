from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('Union And UnionAll').getOrCreate()

    emp_data = [
        ('James', 'Sales', 'NY', 90000, 34, 10000),
        ('Michael', 'Sales', 'NY', 86000, 56, 20000),
        ('Robert', 'Sales', 'CA', 81000, 30, 23000),
        ('Maria', 'Finance', 'CA', 90000, 24, 23000)
    ]

    emp_schema = ['emp_name', 'department', 'state', 'salary', 'age', 'bonus']

    empDF = spark.createDataFrame(data=emp_data, schema=emp_schema)

    dept_Data = [
        ('James', 'Sales', 'NY', 90000, 34, 10000),
        ('Maria', 'Finance', 'CA', 90000, 24, 23000),
        ('Jen', 'Finance', 'NY', 79000, 53, 15000),
        ('Jeff', 'Marketing', 'CA', 80000, 25, 18000),
        ('kumar', 'Marketing', 'NY', 91000, 50, 21000)
    ]

    deptSchema = ['emp_name','department', 'state', 'salary', 'age', 'bonus']

    deptDF = spark.createDataFrame(data=dept_Data, schema=deptSchema)

# Merge two or more Dataframe using union
    #   empDF.union(deptDF).show(truncate=False)

# Merge two or more Dataframe using unionAll
    #   empDF.unionAll(deptDF).show(truncate=False)

# Merge without Duplicate
    #   empDF.union(deptDF).distinct().show()

#  __________use union:unionByName:unionByName with AllowMissingColumns__________

# Create DataFrames with the different number of columns
    data = [
        (1, 'ABC'),
        (2, 'PQR'),
        (3, 'VUW'),
        (4, 'SUV')
    ]
    df = spark.createDataFrame(data, ['id', 'name'])

    data1 = [
        ('SUV', 8),
        ('KJL', 6),
        ('KGN', 7),
        ('VES', 3)
    ]
    df1 = spark.createDataFrame(data1, ['name', 'id'])

    data2 = [
        ('GHJ',),
        ('JKL',),
        ('MNO',),
        ('KIN',)
    ]
    df2 = spark.createDataFrame(data2, ['name'])
    # union
    df.union(df1).show()

    # UnionByName
    df.unionByName(df1).show()

    # UnionByName use AllowMissingColumns
    df1.unionByName(df2, allowMissingColumns=True).show()

    spark.stop()
    #