from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('unpivot').getOrCreate()

    df = spark.read.csv(r'C:\PYSPARK\04praticesdataInput\03Pivot.csv', header=True, inferSchema=True)
    df.show()

    pivotDf1 = df.groupBy('country').pivot('product').sum('amount')
    pivotDf1.show()

    unpivotDf = pivotDf1.select('country', expr("stack(4,'banana',banana,'beans',beans,'carrots',carrots,'orange',orange)\
     as (product,price)")).where('price is not null')
    unpivotDf.show(truncate=False)

    spark.stop()
    #