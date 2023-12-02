from pyspark.sql import SparkSession
from pyspark.sql.functions import lower

# Assignment No:02
# Assignment description: Filter Data Frame
# Date:2023-05-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Data Frame with column" the application name
    spark = SparkSession.builder.master('local[*]').appName('Filter Data Frame').getOrCreate()

    # input on top csv file
    input_df = spark.read.csv(r'C:\PYSPARK\04CSVFiles\homes.csv', header=True,inferSchema=True)

    # this line calculate the total count of records in the dataframe
    input_df.show()

    # print the schema of csv files
    # input_df.printSchema()

    # filter the

    # input_df.filter("Beds >= 5.0").show()
    # input_df.filter('Age >= 40'). show()

    # filter the records with select specific columns

    # input_df.filter('Taxes >= 5000').select('Taxes').show()
    # input_df.filter('Rooms=8.0').select(input_df.Rooms).show()
    # input_df.filter(input_df.Rooms == 8.0).show()

    # select the data first then filter the data

    # input_df.select('*').filter(input_df.Rooms == 8.0).show()
    # input_df.select('*').filter('Rooms=8.0').show()

    # &:AND and |:OR
    #input_df.filter((input_df.Rooms == 8.0) | (input_df.Rooms == 10.0)).show()

    # Startswith and Endswith filter records
    input_df.filter((input_df.Sell).startswith('17')).show()

    # terminated spark session
    spark.stop()

    #