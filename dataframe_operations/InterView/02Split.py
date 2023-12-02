from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Split").getOrCreate()

    df = spark.read.csv(r"C:\PYSPARK\04praticesdataInput\01orderby.csv", header=True, inferSchema=True)

    df.show(truncate=False)

    

    spark.stop()