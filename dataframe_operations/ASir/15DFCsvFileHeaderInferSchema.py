from pyspark.sql import SparkSession

# Assignment No:14
# Assignment Description : Simple Data Frame csv File Collect ,Header,inferSchema
# Date:-2023-22-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "DF With header,inferSchema" as the Application Name
    spark = SparkSession.builder.master("local[*]").appName("DF With header,inferSchema").getOrCreate()

    # input on top of csv file
    input_df = spark.read.csv(r"C:\PYSPARK\02dataframeIPcsv\addresses.csv", header=True, inferSchema=True)

    # This line calculates the total count of records in the dataframe
    print(input_df.show())

    # terminated spark session
    spark.stop()

