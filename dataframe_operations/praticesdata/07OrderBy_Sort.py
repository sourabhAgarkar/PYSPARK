from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('order by() sort()').getOrCreate()

    df = spark.read.csv(r'C:\PYSPARK\04praticesdataInput\01orderby.csv', header=True,inferSchema=True)
    df.show()
# 1
    # dataframe sorting using the sort () function
    #   df.sort('salary','age').show(truncate=False)
    #   df.sort(col('salary'),col('age')).show(truncate=False)

# 2
    # dataframe sorting using orderBy() function
    #   df.orderBy('age', 'bonus').show()
    #   df.orderBy(col('age'), col('bonus')).show()

# 3
    # sort by Ascending (ASC)
    #   df.sort(df.salary.asc(), df.age.asc()).show(truncate=True)
    #   df.sort(col('salary').asc()).show()
# 4

    # dataframe sorting using orderBy() function (ASC)
    #   df.orderBy(df.salary.asc()).show()
    #   df.orderBy(col('salary').asc()).show()

# 5
    # Sort by Descending (DESC)
    #   df.sort(df.salary.desc()).show()
    #   df.sort(col('salary').desc(), col('age').desc()).show()

# 6
    # orderby Descending(DESC)
    #   df.orderBy(df.salary.desc()).show()
    #   df.orderBy(col('salary').desc()).show()

# 7
    # Using Raw SQL
    df.createOrReplaceTempView('emp')
    #   spark.sql('select * from emp order by salary desc').show(truncate=False)

    df.createTempView('emp1')
    #   spark.sql('select * from emp1').show()

# 8
    # asc_nulls_first() and asc_nulls_last()

    df.orderBy(col('salary').asc_nulls_first()).show()
    df.orderBy(col('salary').asc_nulls_last()).show()

    #   desc_nulls_first() and desc_nulls_last()

    df.orderBy(col('salary').desc_nulls_first()).show()
    df.orderBy(col('salary').desc_nulls_last()).show()

    spark.stop()
    #