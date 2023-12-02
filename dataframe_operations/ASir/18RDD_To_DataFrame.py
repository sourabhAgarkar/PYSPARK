from pyspark.sql import SparkSession

# Assignment No:17
# Assignment Description : RDD TO Data Frame,2nd scenario RDD To Data Frame
# Date:-2023-22-06

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "RDD TO Data Frame" the application name
    spark = SparkSession.builder.master("local[*]").appName("RDD TO Data Frame").getOrCreate()

    input_dict = [{"id": 1, "name": "ABC", "city": "pune"}, {"id": 2, "name": "PQR", "city": "nagpur"},
                  {"id": 3, "name": "XYZ", "city": "mumbai"}, {"id": 4, "name": "JCB", "city": "delhi"}
                 ]

    # Parallelize input_dict to create an RDD
    input_rdd = spark.sparkContext.parallelize(input_dict)

    # Converting RDD to DataFrame
    input_df = input_rdd.toDF()

    # Printing and displaying the DataFrame
    input_df.show()

# 2nd scenario

    print("_______2nd scenario rdd to dataframe_______")

    input_dict = [{"id": 1, "name": "ABC", "city": "pune"}, {"id": 2, "name": "PQR", "city": "nagpur"},
                  {"id": 3, "name": "XYZ", "city": "mumbai"}, {"id": 4, "name": "JCB", "city": "delhi"}
                  ]

    # Parallelize input_dict to create an RDD
    input_rdd1 = spark.sparkContext.parallelize(input_dict)

    # Converting RDD to DataFrame
    input_df1 = spark.createDataFrame(input_rdd1)

    # Printing and displaying the DataFrame
    input_df1.show()

    # terminated spark session
    spark.stop()

    #