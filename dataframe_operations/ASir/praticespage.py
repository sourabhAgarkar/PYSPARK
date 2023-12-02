from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col,sum,min,max,avg
from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import when

# Assignment No : 01
# Assignment description: read data oracle sql developer to PyCharm
# Date:2023-15-07

if __name__ == '__main__':

    # Set up SparkSession with Oracle JDBC driver configuration
    spark = SparkSession.builder.master('local[*]')\
        .appName("spark_Oracle_config")\
        .config("spark.jars", r"C:\Spark\spark-3.3.2-bin-hadoop2\jars\ojdbc8.jar")\
        .getOrCreate()

    # JDBC URL for connecting to Oracle database
    jdbc_url = "jdbc:oracle:thin:@ 192.168.41.118:1521/xe"

    # Connection properties for connecting to Oracle database
    connection_properties = {
        'user': 'sys as sysdba',
        'password': '123',
        'driver': 'oracle.jdbc.driver.OracleDriver'
    }

    # Read data from the "employees1" table in the Oracle database
    ser_det = spark.read.jdbc(url=jdbc_url, table="ser_det", properties=connection_properties)
    customer1 = spark.read.jdbc(url=jdbc_url, table="customer1", properties=connection_properties)
    vendors = spark.read.jdbc(url=jdbc_url, table="Vendors", properties=connection_properties)
    employee = spark.read.jdbc(url=jdbc_url, table="employee", properties=connection_properties)
    sparepart = spark.read.jdbc(url=jdbc_url, table="sparepart", properties=connection_properties)
    purchase = spark.read.jdbc(url=jdbc_url, table="purchase", properties=connection_properties)

    # ser_det.show()
    # customer1.show()
    # vendors.show()
    # employee.show()
    # sparepart.show()
    # purchase.show()

    ser_det.join(customer1, on="cid", how="fullOuter")\
        .join(employee, on="eid", how="fullOuter")\
        .join(sparepart, on="spid", how="fullOuter").join(purchase, on="spid", how="fullOuter")\
        .join(vendors, on="vid", how="fullOuter")

    # employee.withColumn("name", employee["ename"])

    # employee.withColumn("Bio", lit("Agar"))

    employee.groupBy("EID").agg(sum("E_SAL_"),
                                min("E_SAL_"),
                                max("E_SAL_"),
                                avg("E_SAL_"))

    employee.withColumn("row_number", row_number().over(Window.orderBy(col("E_SAL_").desc())))

    employee.select("*", lit("Sourabh").alias("Name"))

    employee.withColumn("Name", lit("Sourabh"))

    employee.withColumn("Grade", when( (col("E_SAL_") >= 1000 & col("E_SAL_") <= 1200) , "A"))
    # spark session terminate
    spark.stop()
    #