from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, max

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
    gender = spark.read.jdbc(url=jdbc_url, table="gender", properties=connection_properties)
    greatest_list = spark.read.jdbc(url=jdbc_url, table="greatest_list", properties=connection_properties)
    table1_data = spark.read.jdbc(url=jdbc_url, table="table1_data", properties=connection_properties)
    table2_data = spark.read.jdbc(url=jdbc_url, table="table2_data", properties=connection_properties)
    employees1 = spark.read.jdbc(url=jdbc_url, table="employees1", properties=connection_properties)
    departments1 = spark.read.jdbc(url=jdbc_url, table="departments1", properties=connection_properties)
    student_marks = spark.read.jdbc(url=jdbc_url, table="student_marks", properties=connection_properties)
    duplicate_data = spark.read.jdbc(url=jdbc_url, table="duplicate_data", properties=connection_properties)

    gender.withColumn("Gender", when(col("A") == 'F', 'Male')
                      .when(col("A") == 'M', 'Female')
                      .otherwise(col('A')))

    gender.select("A", when(col("A") == 'F', 'male')
                  .when(col("A") == 'M', 'Female')
                  .otherwise(col("A")).alias("Gender"))

    gender.createOrReplaceTempView("gender")

    spark.sql("select A,"
              " case"
              " when A = 'F'"
              " then 'male'"
              " else 'Female'"
              " end Gender"
              " from gender")

    greatest_list.createOrReplaceTempView("greatest_list")

    spark.sql("select coalesce(col1,col2,col3) from greatest_list")

    # joins
    table1_data.join(table2_data, on="col1", how="inner")
    table1_data.join(table2_data, table1_data.COL1 == table2_data.COL1, "inner")

    # leftAnti
    table1_data.join(table2_data, on="col1", how="leftAnti")

    # using SQL expression
    employees1.createOrReplaceTempView("emp")
    departments1.createOrReplaceTempView("dep")

    spark.sql("select *"
              " from emp inner join dep"
              " on emp.department_id = dep.department_id")

    student_marks.createOrReplaceTempView("student_marks")

    spark.sql("""
    select 
        substr(name,1,instr(name," "))Fname,
        substr(name,instr(name," "))lname,
        marks,
        case
            when marks >= 95
                then 'A'
            when marks >= 70
                then 'B'
            when marks >= 55
                then 'C'
            when marks >= 38
                then 'D'
            else 'F'
        end Grade
    from student_marks
    """)
    # Q1 How to select unique records from a table using analytical function
    duplicate_data.select("COL1").distinct()

    duplicate_data.union(duplicate_data).distinct()

    employees1.select(max("salary")).show()

    # spark session terminate
    spark.stop()

    #