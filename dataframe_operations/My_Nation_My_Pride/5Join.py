from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, min, avg, mean, sum

# Assignment:4
# Assignment description: Practise_Questions
# Date:2023-08-07


if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "Schema in pyspark" as the Application Name
    spark = SparkSession.builder.master('local[*]').appName('Join').getOrCreate()

    # input on top of csv file
    csv_file = spark.read.csv(r"C:\PYSPARK\04CSVFiles\freshman_kgs.csv", header=True, inferSchema=True)

    # input on top of csv file
    csv_file1 = spark.read.csv(r"C:\PYSPARK\04CSVFiles\freshman_lbs.csv", header=True, inferSchema=True)

    # inner join
    csv_file.join(csv_file1, on='Sex', how='inner').show()

    # cross join
    csv_file.join(csv_file1, on='Sex', how='cross').show()

    # outer join
    csv_file.join(csv_file1, on='Sex', how='outer').show()

    # full join
    csv_file.join(csv_file1, on='Sex', how='full').show()

    # full_outer join
    csv_file.join(csv_file1, on='Sex', how='full_outer').show()

    # left join
    csv_file.join(csv_file1, on='Sex', how='left').show()

    # left_outer join
    csv_file.join(csv_file1, on='Sex', how='left_outer').show()

    # right join
    csv_file.join(csv_file1, on='Sex', how='right').show()

    # right join
    csv_file.join(csv_file1, on='Sex', how='right_outer').show()

    # nth

    # create df on top of the csv file
    df = spark.read.csv(r'C:\PYSPARK\Data\homes.csv', header=True, inferSchema=True)

    # maximum salary
    df.withColumn('max_salary', max('Taxes').over(Window.partitionBy('Beds').orderBy(col('Taxes').desc()))).show()

    # minimum salary
    df.withColumn('max_salary', min('Taxes').over(Window.partitionBy('Beds').orderBy(col('Taxes').desc()))).show()

    # cast salary

    # create df on top of the csv file
    df = spark.read.csv(r'C:\PYSPARK\04CSVFiles\cities.csv', header=True, inferSchema=True)

    # avg(col): returns the average of the values in a group
    df.select(avg('LonD')).show()

    # min(col) Returns the minimum value of the expression in a group
    df.select(min('LonD')).show()

    # max(col): returns the maximum value of the expression in a group.
    df.select(max('LonD')).show()

    # mean(col) Returns the average of the values in a group
    df.select(mean('LonD')).show()

    # sum(col) : Returns the sum of all values in the expression
    df.select(sum('LonD')).show()

    # terminate spark session
    spark.stop()

