from pyspark.sql import SparkSession

# Assignment No:16
# Assignment Description : Data Frame Json File
# Date:-2023-22-06

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Data Frame Json File" as the application name
    spark = SparkSession.builder.master("local[*]").appName("Data Frame Json File").getOrCreate()

    # take data in 16DFOverWrite from json file l-24
    json_df = spark.read.json(r"C:\PYSPARK\03DFOutput\02JsonFileCreate")

    # This line calculates the total count of records in json file
    print(json_df.show())

    # Data Frame Schema with columns and data types
    print(json_df.printSchema())

# terminated spark session
    spark.stop()