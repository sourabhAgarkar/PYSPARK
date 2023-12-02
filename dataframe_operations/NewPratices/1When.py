from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import expr, col

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

    df.withColumn("Id", when(df["department_id"] == 10, 'Ten')
                  .when(df["department_id"] == 20, "Twenty")
                  .when(df["department_id"] == 30, "Thirty")
                  .otherwise(df["department_id"]))

    df.select("department_id", when(df["department_id"] == 10, "Ten")
              .when(df["department_id"] == 20, "Twenty")
              .when(df["department_id"] == 50, "Fifty")
              .otherwise(df["department_id"]) .alias("Word"))

    df.select("department_id", expr("""
        case
            when department_id = 10
                then 'Ten'
            when department_id = 20
                then 'Twenty'
            when department_id = 50
                then 'Fifty'
            else department_id
        end Word
    """))

    df.withColumn("Word", expr("""
        case
            when department_id = 10
                then 'Ten'
            when department_id = 50
                then 'Fifty'
            when department_id = 70
                then 'Seventy'
            else department_id
        end
    """))

    df.createOrReplaceTempView("employees1")
    spark.sql("""
        select department_id,
            case
                when department_id = 50
                    then 'Fifty'
                when department_id = 70
                    then 'Seventy'
                when department_id = 30
                    then 'Thirty'
                else department_id
            end Word
        from employees1
    """)



    # spark session terminate
    spark.stop()

    #