from pyspark.sql import SparkSession

# Assignment No:17
# Assignment Description :Simple Data Frame csv File Collect,covert column data type,creating new column,rename column,
# Drop column
# Date:-2023-23-06

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Data Frame with column" the application name
    spark = SparkSession.builder.master("local[*]").appName("Data Frame with Column").getOrCreate()

    # dataframe on top of csv file
    input_df = spark.read.csv(r"C:\PYSPARK\02dataframeIPcsv\addresses.csv", header=True, inferSchema=True)

    # This line calculates the total count of records in the dataframe
    input_df.show()


# Convert column data type

    from pyspark.sql.functions import col
    # Convert "postcode" column to integer
    print("---------postcode column string to integer-----------")
    new_df = input_df.withColumn("postcode", col("postcode").cast("integer"))

    # Show the results
    new_df.printSchema()

    # postcode +1000  postcode change
    print("----------postcode +1000----------")
    add_df = new_df.withColumn("postcode", col("postcode")+1000)
    add_df.show()

    # creating new column
    print("----------creating new column (new_postcode)----------")
    create_new_col = new_df.withColumn("new_postcode", col("postCode"))
    create_new_col.show()

    # Rename Column
    print("----------Rename Column (new_name_postcode)----------")
    rename_col = create_new_col.withColumnRenamed("new_postcode", "new_name_postcode")
    rename_col.show()

    # drop column
    print("----------drop column (new_name_postcode)----------")
    rename_col.drop("new_name_postcode").show()

    # terminated spark session
    spark.stop()