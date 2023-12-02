from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("RDD Question").getOrCreate()

    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\02dataframeIPcsv\addresses1.txt")

    print("fetch id,first_name,last_name")

    columns_rdd = input_rdd.map(lambda line: line.split(",")).map(lambda cols: (cols[0], cols[1], cols[2]))

    for store in columns_rdd.collect():
        print(store)

    print("first_name start with 'J' or ends with 's' ")
    filter_rdd = input_rdd.filter(lambda line: line.split(",")[1].startswith('J') or line.split(",")[2].endswith('s'))

    for row in filter_rdd.collect():
        print(row)

    print("id,first_name,last_name,city wise minimum salary")

    city_salary_rdd = input_rdd.map(lambda line: line.split(",")[3:5])

    print(city_salary_rdd.collect())

    # terminated spark session
    spark.stop()

    #
