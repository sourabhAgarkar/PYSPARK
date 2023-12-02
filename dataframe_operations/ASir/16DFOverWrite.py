from pyspark.sql import SparkSession

# Assignment No:15
# Assignment Description : Simple Data Frame read csv File Collect->Header->inferSchema->overwrite->csvFile->jsonFile
# Date:-2023-22-06

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "DF overWrite" as the Application Name
    spark = SparkSession.builder.master("local[*]").appName("DF OverWrite").getOrCreate()

    # input on top of csv file
    input_df = spark.read.csv(r"C:\PYSPARK\02dataframeIPcsv\addresses.csv", header=True, inferSchema=True)

    # This line calculates the total count of records in dataframe
    print(input_df.show())

    # print(input_df.printSchema())

    # This line saves the DataFrame as a csv file
    input_df.write.mode("OverWrite").csv(r"C:\PYSPARK\03DFOutput\01CsvFileCreate")

    # This line saves the DataFrame as a json file
    input_df.write.mode("overwrite").json(r"C:\PYSPARK\03DFOutput\02JsonFileCreate")

    # terminated spark session
    spark.stop()