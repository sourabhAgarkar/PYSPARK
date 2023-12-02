from pyspark.sql import SparkSession

# Assignment No:18
# Assignment Description : Questions on saturday
# Date:- 2023-24-06

if __name__ == '__main__':

    # Create a SparkSession with local[*] as the master and "DF With header,inferSchema" as the Application Name
    spark = SparkSession.builder.master("local[*]").appName("Question Set").getOrCreate()

    # input on top of file
    input_rdd = spark.read.csv(r"C:\PYSPARK\02dataframeIPcsv\addresses1.txt", header=True, inferSchema=True)

    # Display the contents of the input RDD
    print(input_rdd.show())

    # selecting the desired columns
    selected_columns = input_rdd.select("id", "first_name", "last_name")

    # Display the selected columns
    print(selected_columns.show())

    # filter records
    filter_record = input_rdd.filter((input_rdd["first_name"].startswith("S")) | (input_rdd["last_name"].endswith("s")))

    # display the filter records
    print(filter_record.show())

    # Retrieve id, first_name, last_name, and minimum salary grouped by city
    minimum_salary = input_rdd.groupBy("city").agg({"salary": "min"}).withColumnRenamed("min(salary)", "minimum_salary")

    # Display minimum salary
    print(minimum_salary.show())

    # Retrieve id, first_name, last_name, and maximum salary grouped by city
    maximum_salary = input_rdd.groupBy("city").agg({"salary": "max"}).withColumnRenamed("max(salary)", "maximum_salary")

    # display maximum salary
    print(maximum_salary.show())

    # terminated spark session
    spark.stop()

    #