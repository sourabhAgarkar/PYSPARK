from pyspark.sql import SparkSession

# Assignment No:17
# Assignment Description : RDD TO Data Frame,4th scenario RDD To Data Frame ,2)select diff select col statement
# Date:-2023-23-06

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Data Frame Select" the application name
    spark = SparkSession.builder.master("local[*]").appName("Data Frame Select").getOrCreate()

    input_data = [
        ("jack", "thomas", "uk"),
        ("john", "sena", "usa"),
        ("carry", "alex", "india"),
        ("thomas", "alva", "aus")
    ]

    # Parallelize input_data into an RDD
    input_rdd = spark.sparkContext.parallelize(input_data)

    # Print the contents of input_rdd
    print(input_rdd.collect())

    # Create a DataFrame from input_rdd
    dataframe = spark.createDataFrame(input_rdd, ["first_name", "last_name", "country"])

    # Display the contents of the DataFrame
    dataframe.show()

# 2)select diff select col statement

    # 1)
    # Select the "first_name" column from the DataFrame
    dataframe.select("first_name").show()

    # 2)
    # Select specific columns from the DataFrame
    dataframe.select(dataframe.first_name, dataframe.country).show()

    # 3)
    # Select specific columns from the DataFrame using col()
    from pyspark.sql.functions import col
    dataframe.select(col("first_name"), col("country")).show()

    # terminated spark session
    spark.stop()