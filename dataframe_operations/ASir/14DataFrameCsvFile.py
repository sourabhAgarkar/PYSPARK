from pyspark.sql import SparkSession

# Assignment No:13
# Assignment Description : Simple Data Frame csv File Collect
# Date: 2023-22-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "DataFrameCsvFile" as the application name
    spark = SparkSession.builder.master("local[*]").appName("DataFrameCsvFile").getOrCreate()

    # DataFrame on top of csv file
    input_df = spark.read.csv(r"C:\PYSPARK\02dataframeIPcsv\addresses.csv")

    # This line calculates the total count of records in the DataFrame
    input_df.show()

    # shows how many columns in df and its data type
    #input_df.printSchema()

    # terminated spark session
    spark.stop()