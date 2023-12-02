from pyspark.sql import SparkSession

# Assignment  No:6
# Assignment description:Get length of each character in input file
# Date:2023-20-06

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("length_element").getOrCreate()

    # create rdd from input text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\text.txt")

    # split input rdd content,to get each element from rdd
    split_rdd = input_rdd.flatMap(lambda x: x.split(" "))

    # get length of each element
    len_rdd = split_rdd.map(lambda x: (x, len(x)))

    # display the result
    # print(len_rdd.collect())

# OR
    # display the result
    for element,length in len_rdd.collect():
        print(f"element:{element},length:{length}")


# terminated spark session
    spark.stop()

