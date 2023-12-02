from pyspark.sql import SparkSession

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Data Frame Select" the application name
    spark = SparkSession.builder.master("local[*]").appName("Data Frame Select").getOrCreate()

    input_data = [
        ("01", "jack", "thomas", "uk", 2000),
        ("02", "john", "sena", "usa", 3000),
        ("03", "carry", "alex", "india", 4000),
        ("04", "thomas", "alva", "aus", 5000)
    ]

    # Parallelize input_data into an RDD
    input_rdd = spark.sparkContext.parallelize(input_data)

    # Print the contents of input_rdd
    #print(input_rdd.collect())

    # Create a DataFrame from input_rdd
    dataframe = spark.createDataFrame(input_rdd, ["id", "first_name", "last_name", "country", "salary"])

    # Display the contents of the DataFrame
    dataframe.show()

    # using where with between
    print("___________salary between 2000 and 5000__________")
    dataframe.select("salary").where(dataframe.salary.between(2000, 5000)).show()

    # by using an aggregate function
    print("___________avg salary per country__________")
    dataframe.groupby("country").avg("salary").show()

    # terminated spark session
    spark.stop()