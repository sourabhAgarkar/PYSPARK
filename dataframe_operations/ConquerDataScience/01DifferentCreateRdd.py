from pyspark.sql import SparkSession

if __name__ == '__main__':

    # initialize spark session
    spark = SparkSession.builder.master('local[*]').appName('Different RDD').getOrCreate()

    # creating RDD 's from parallelize collection
    data = [1, 2, 3, 4, 5, 6, 7, 8]

    input_rdd = spark.sparkContext.parallelize(data)

    print(input_rdd.collect())

    # Create RDD from external data source
    text_input_rdd = spark.sparkContext.textFile(r'C:\PYSPARK\Data\text.txt')

    print(f"create rdd external:{text_input_rdd.collect()}")

    # read entire file into a rdd as single record
    whole_input_rdd = spark.sparkContext.wholeTextFiles(r'C:\PYSPARK\Data\input_data.txt')

    print(f"read entire file:{whole_input_rdd.collect()}")

    # create an empty rdd using sparkContext.emptyRDD
    empty_rdd = spark.sparkContext.emptyRDD()

    print(f"empty rdd:{empty_rdd.collect()}")

    # Repartition and Coalesce
    print(f"Number of partition:{input_rdd.getNumPartitions()}")

    repartition_rdd = input_rdd.repartition(4)
    print(f"Number of partition after repartition:{repartition_rdd.getNumPartitions()}")

    repartition_coal_rdd = input_rdd.coalesce(2)
    print(f"Number of partition after coalesce:{str(repartition_coal_rdd.getNumPartitions())}")

    # print(type(repartition_coal_rdd))

    # pyspark read csv file into dataframe
    df = spark.read.csv(r"C:\PYSPARK\Data\homes.csv", header=True, inferSchema=True)
    df.printSchema()

    # read multiple csv files
    file_path =[
        r"C:\PYSPARK\Data\addresses.csv",
        r"C:\PYSPARK\Data\emp_data.csv"
    ]
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    #df.show()

    # Read all csv files in a directory
    read_file = spark.read.csv(r"C:\BrainWorks\003SPARK\CSVFiles", header=True)

    read_file.show()

    # terminated spark session
    spark.stop()

    #