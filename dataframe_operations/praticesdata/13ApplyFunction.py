from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, lower, concat_ws, substring, trim, length, split
from pyspark.sql.functions import sqrt, exp, log, pow,ceil, floor, round, trunc
if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('Apply Function to Column').getOrCreate()

    data = [
        (1, 'John Jones'),
        (2, 'tracey smith'),
        (3, 'amy sanders')
    ]
    df = spark.createDataFrame(data, ['id', 'name'])

# Apply function using withColumn()
    df.withColumn('upperName', upper('name')).show()

# Apply function using select:upper,lower,concat_ws,substring,trim,length,split

    df.select(upper('name')).show()
    df.select(lower('name')).show()
    df.select(concat_ws('_', 'id', 'name').alias('id_name')).show()
    df.select(substring('name', 1, 1).alias('name')).show()
    df.select(trim('name')).show()
    df.select(length('name').alias('count')).show()
    df.select(split('name', ' ')).show()

# Apply function using sql()

    df.createOrReplaceTempView('emp')
    spark.sql('select id,upper(name) from emp').show()

# Mathematical functions:-sqrt,exp,log,pow,ceil,floor,round trunc
    df.select(sqrt('id')).show()
    df.select(exp('id')).show()
    df.select(log('id')).show()
    df.select(pow('id', 2)).show()
    df.select(ceil('id')).show()
    df.select(floor('id')).show()
    df.select(round('id', 1)).show()

    spark.stop()
    #