from pyspark.sql import SparkSession

# Assignment no 1
# Assignment description: first collect text,split the data,length of data,print in for loop
# Date: 2023-19-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("imp_data").getOrCreate()

    # create rdd from input text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\input_data.txt")

    # print the text data
    print(input_rdd.collect())

    # split input rdd content,To get each element from rdd
    split_rdd = input_rdd.flatMap(lambda x: x.split(" "))

    # split the data
    print(split_rdd.collect())

    # get length of each element
    len_rdd = split_rdd.map(lambda x: (x, len(x)))

    # count the length of word
    print(len_rdd.collect())

    # display result
    for input, length in len_rdd.collect():
        print(f"element:{input} , length:{length}")

# terminated spark session
    spark.stop()

    #