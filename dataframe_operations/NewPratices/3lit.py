from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark.sql.functions import expr

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

    # simple usage of lit() function
    df.select("employee_id", lit(1).alias("Number"))

    # lit function with WithColumn
    df.select("employee_id").withColumn("Number", lit(1))

    df.withColumn("Grade", when((df["Salary"] >= 10000) & (df["Salary"] <= 20000), lit("Saurabh"))
                  .otherwise(lit("Agarkar"))).select("Grade")

    df.withColumn("Bio", expr("""
        concat(concat(first_name,' '),last_name)
    """)).select("Bio")

    spark.stop()
    #