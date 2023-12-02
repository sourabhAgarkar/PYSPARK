from pyspark.sql import SparkSession

# Assignment No: 11
# Assignment Description BroadCast Variables BroadCast Variables are read only Variables,that are cached and available on
# all the node in cluster in order to access and use by the task.
# Date:2023-21-06

if __name__ == '__main__':

    # initialization the spark session
    spark = SparkSession.builder.master("local[*]").appName("BroadCaseVarible").getOrCreate()

    # creating rdd from input text file
    input_rdd = spark.sparkContext.textFile(r"C:\PYSPARK\Data\Student_Deatails")

    # print the text data
    print(input_rdd.collect())

    #
    city = {"OS": "Osmanabad", "LT": "Latur", "PU": "Pune", "RN": "Ranjani"}

    # broadcast variable Declared
    broad_cast = spark.sparkContext.broadcast(city)

    # print broadcast Variable
    print(broad_cast.value)

    # split input rdd content ,to get each element from rdd
    split_rdd = input_rdd.map(lambda x: x.split(","))

    # split the data
    print(split_rdd.collect())

    # Map each element in split_rdd to a new list with the third element replaced by the corresponding value from broad_cast
    # Convert each element in the RDD to a string by joining the elements with commas
    replace_rdd = split_rdd.map(lambda x: [x[0], x[1], broad_cast.value[x[2]]]).map(lambda x: ",".join(x))

    # replace rdd
    print(replace_rdd.collect())

    # terminated spark session
    spark.stop()