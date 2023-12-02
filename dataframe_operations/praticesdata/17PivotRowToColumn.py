from pyspark.sql import SparkSession
from pyspark.sql.functions import max, sum
if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('Pivot() Row to Column').getOrCreate()

    data = [
        ('Banana', 1000, 'USA'), ('Carrots', 1500, 'USA'), ('Beans', 1600, 'USA'), \
        ('Orange', 2000, 'USA'), ('Orange', 2000, 'USA'), ('Banana', 400, 'China'), \
        ('Carrots', 1200, 'China'), ('Beans', 1500, 'China'), ('Orange', 4000, 'China'), \
        ('Banana', 2000, 'Canada'), ('Carrots', 2000, 'Canada'), ('Beans', 2000, 'Mexico')
    ]

    dataDf = spark.createDataFrame(data, ['Product', 'Amount', 'Country'])

    dataDf.groupBy('Product').pivot('Country').agg(max('Amount')).show()

# OR ---

    dataDf.groupBy('Product', 'Country').sum('Amount').groupBy('Product').pivot('Country').sum('sum(Amount)').show()

    spark.stop()
    #