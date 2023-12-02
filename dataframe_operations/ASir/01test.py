from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate()

# header with false take by default column names
# csv_read1 = spark.read.csv(r"C:\Users\91876\Desktop\addresses.csv", header=True, inferSchema=True).show()

# header by default
# print(csv_read1)

# built column name added it with inferSchema
# csv_read2=spark.read.csv(r"C:\PYSPARK\Data\addresses.csv",inferSchema=True).show()

# print(csv_read2)

# built column name with inferSchema=False
csv_read3 = spark.read.csv(r"C:\PYSPARK\Data\addresses.csv", inferSchema=False).show()


print(csv_read3)


# with use the inferSchema
# csv_read4=spark.read.csv(r"C:\PYSPARK\Data\addresses.csv").show()

# print(csv_read4)


spark.stop()