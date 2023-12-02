from pyspark.sql import SparkSession
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

    df.withColumn("Bio", expr("""
        first_name ||' '|| last_name
    """)).select("Bio")

    df.withColumn("Bio", expr("""
        concat(first_name,last_name)
    """)).select("Bio")

    # SQL case when

    df.withColumn("Bio", expr("""
        case
            when department_id = 50
                then 'Fifty'
            else department_id
        end
    """)).select("Bio")

    # Providing alias using as

    df.select("first_name", "last_name", expr("""
        concat(concat(first_name,' '),last_name) Bio
    """))

    df.select("employee_id", "manager_id").filter(expr("employee_id == manager_id"))

    df.select("Salary").filter(expr("""
        salary >= 20000
    """))

    df.select("employee_id", "manager_id")

    spark.stop()

    #