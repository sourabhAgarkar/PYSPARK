from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Assignment No : 01
# Assignment description: read data oracle sql developer to PyCharm Community Edition 2023.1.2
# Date:2023-01-07

if __name__ == '__main__':

    # Set up SparkSession with Oracle JDBC driver configuration
    spark = SparkSession.builder.master('local[*]')\
    .appName("spark_Oracle_config")\
    .config("spark.jars", r"C:\Spark\spark-3.3.2-bin-hadoop2\jars\ojdbc8.jar")\
    .getOrCreate()

    # JDBC URL for connecting to Oracle database
    jdbc_url = "jdbc:oracle:thin:@192.168.0.57:1521/xe"

    # Connection properties for connecting to Oracle database
    connection_properties = {
        "user": "sys as sysdba",
        "password": "123",
        "driver": "oracle.jdbc.driver.OracleDriver"
    }

    # Read data from the "employees1" table in the Oracle database
    df = spark.read.jdbc(url=jdbc_url, table="employees1", properties=connection_properties)

    df.show()

    # Read data from the "departments1" table in the Oracle database
    df1 = spark.read.jdbc(url=jdbc_url, table="departments1", properties=connection_properties)

    df1.show()

    # select employee_id concat(first_name,last_name)salary
    store = df.select("employee_id", concat_ws(" ", "first_name", "last_name").alias("full name"), "salary", "department_id")

    store.show()

    # get emp name who salary is greater than 3000 and less than 5000
    df.select(df.FIRST_NAME, df.LAST_NAME, df.SALARY).where(df.SALARY.between(3000, 5000)).show()

    # get emp details who first name start with p or ends with n and has salary between 3000 and 5000
    df.filter((df["first_name"].startswith("P")) | (df["first_name"].endswith("n"))).where(df.SALARY.between(3000, 5000)).show()

    # join employees1 and departments1 tables
    df.join(df1, on='manager_id', how='inner').show()

    # get emp details who
    # terminate spark session
    spark.stop()

    #