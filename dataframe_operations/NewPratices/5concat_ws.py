from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws

if __name__ == '__main__':

    # Set up SparkSession with Oracle JDBC driver configuration
    spark = SparkSession.builder.master('local[*]')\
        .appName("spark_Oracle_config")\
        .config("spark.jars", r"C:\Spark\spark-3.3.2-bin-hadoop2\jars\ojdbc8.jar")\
        .getOrCreate()

    # JDBC URL for connecting to Oracle database
    jdbc_url = "jdbc:oracle:thin:@192.168.71.118:1521/xe"

    # Connection properties for connecting to Oracle database
    connection_properties = {
        'user': 'sys as sysdba',
        'password': '123',
        'driver': 'oracle.jdbc.driver.OracleDriver'
    }

    # Read data from the "employees1" table in the Oracle database
    df = spark.read.jdbc(url=jdbc_url, table="employees1", properties=connection_properties)

    df1 = df.withColumn("bio", expr("""
        first_name || ',' || last_name
    """)).select("bio", split(col("bio"), ",").alias("Name"))

    df1.withColumn("concat_ws", concat_ws(",", col("Name"))).select("concat_ws")

    df1.select("Name", concat_ws(",", col("Name")).alias("Name1"))

    df1.select(concat_ws(",", "Name"))

    spark.stop()
    #