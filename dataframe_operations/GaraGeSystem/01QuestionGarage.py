from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col, min, row_number, trim, sum, count, dense_rank, when,collect_list,\
    date_format,days, dayofmonth
from pyspark.sql.window import Window

# Assignment No : 01
# Assignment description: read data oracle sql developer to PyCharm
# Date:2023-15-07

if __name__ == '__main__':
    # Set up SparkSession with Oracle JDBC driver configuration
    spark = SparkSession.builder.master('local[*]') \
        .appName("spark_Oracle_config") \
        .config("spark.jars", r"C:\Spark\spark-3.3.2-bin-hadoop2\jars\ojdbc8.jar") \
        .getOrCreate()

    # JDBC URL for connecting to Oracle database
    jdbc_url = "jdbc:oracle:thin:@192.168.0.31:1521/xe"

    # Connection properties for connecting to Oracle database
    connection_properties = {
        'user': 'sys as sysdba',
        'password': '123',
        'driver': 'oracle.jdbc.driver.OracleDriver'
    }

    # Read data from the tables in the Oracle database
    customer = spark.read.jdbc(url=jdbc_url, table="customer1", properties=connection_properties)
    vendors = spark.read.jdbc(url=jdbc_url, table='Vendors', properties=connection_properties)
    employee = spark.read.jdbc(url=jdbc_url, table='employee', properties=connection_properties)
    SparePart = spark.read.jdbc(url=jdbc_url, table='sparepart', properties=connection_properties)
    purchase = spark.read.jdbc(url=jdbc_url, table='purchase', properties=connection_properties)
    ser_det = spark.read.jdbc(url=jdbc_url, table='ser_det', properties=connection_properties)

    # Q.1  List all the customers serviced
    # customer.join(ser_det, on='cid', how='inner').show()

    # Q.2  Customers who are not serviced.
    # customer.join(ser_det, on='CID', how='leftAnti').show()

    # Q.3  Employees who have not received the commission.
    # ser_det.filter('comm==0').select('*').show()

    # Q.4  Name the employee who have maximum Commission.
    # join = employee.join(ser_det, on='EID', how='inner')
    # join.filter('COMM != 0').groupBy('ENAME').agg(max('COMM').alias('MAX')).orderBy(col('MAX').desc())\
    #     .show(truncate=False)

    # Q.5  Show employee name and minimum commission amount received by an employee.
    # join = employee.join(ser_det, on='EID', how='inner')
    # join.filter('COMM != 0').groupBy('ENAME').agg(min('COMM').alias('min')).orderBy(col('min').asc())\
    #     .show(truncate=False)

    # Q.6  Display the Middle record from any table.----
    # employee.createOrReplaceTempView('employee')
    # spark.sql('select * from(select row_number() over(order by (select 1) ) as r ,\
    # e.* from employee where r = (select ceil(count(*)/2) from employee)')\
    #     .show()

    # middleRecord = SparePart.rdd.take(SparePart.count()//2)[-1]
    # print(middleRecord)

    # Q.7  Display last 4 records of any table.
    # SparePart.sort(col('SPID').desc()).show(4)

    # Q.8  Count the number of records without count function from any table.
    # print(ser_det.collect()[:])

    # Q.9  Delete duplicate records from "Ser_det" table on cid.(note Please rollback after execution).
    # ser_det.withColumn('row', row_number().over(Window.partitionBy('CID').orderBy('CID'))).where(col('row') > 1)\
    #     .select('CID').show()

    # Q.10 Show the name of Customer who have paid maximum amount
    # customer.join(ser_det, on='CID', how='inner').groupBy('CNAME').agg(max('TOTAL').alias('max')).orderBy(col('max')\
    #                                                                                                       .desc()).show()

    # Q.11 Display Employees who are not currently working.
    # employee.join(ser_det, on='EID', how='leftAnti').show()

    # Q.12 How many customers serviced their two wheelers.
    # Filter = customer.join(ser_det, on='CID', how='inner')
    # Filter.filter("TYP_VEH=='TWO WHEELER' ").show()

    # Q.13 List the Purchased Items which are used for Customer Service with Unit of that Item.
    # purchase.join(ser_det, on='SPID', how='inner').show()

    # Q.14 Customers who have Colored their vehicles.
    # Filter = customer.join(ser_det, on='CID', how='inner')
    # Filter.withColumn('TYP_SER', trim(col('TYP_SER'))).filter("TYP_SER=='COLOR'").show()

    # Q.15 Find the annual income of each employee inclusive of Commission
    # employee.join(ser_det, on='EID', how='inner')\
    #     .withColumn('annual_income', col('E_SAL_')+(col('E_SAL_')*col('COMM')))\
    #     .show()

    # Q.16 Vendor Names who provides the engine oil.
    # vendors.join(purchase, on='VID',  how='inner').join(SparePart, on='SPID', how='inner')\
    #     .select('VNAME', 'SPNAME').filter(col('SPNAME').like("%OIL%")).show()

    # Q.17 Total Cost to purchase the Color and name the color purchased.
    # vendors.join(purchase, on='VID', how='inner').join(SparePart, on='SPID', how='inner')\
    #     .filter(col('SPNAME').like('%COLOUR%')).show()

    # Q.18 Purchased Items which are not used in "Ser_det".
    # purchase.join(ser_det, on='SPID', how='leftAnti').show()

    # Q.19 Spare Parts Not Purchased but existing in Sparepart
    # SparePart.join(purchase, on='SPID', how='leftAnti').show()

    # Q.20 Calculate the Profit/Loss of the Firm. Consider one month salary of each employee for Calculation.
    # ser_det.join(employee, on='EID', how='inner').select(sum('TOTAL')-sum('E_SAL_')).show()

    # Q.21 Specify the names of customers who have serviced their vehicles more than one time.
    # store = customer.join(ser_det, on='CID', how='fullOuter')
    # store.withColumn('dense_rank', dense_rank().over(Window.partitionBy('cname').orderBy(col('cname'))))\
    #     .filter(col('dense_rank') > 1).show()

    # Q.22 List the Items purchased from vendors locationwise.
    # purchase.join(vendors, on='VID', how='inner').join(SparePart, on='SPID', how='inner')\
    #     .select('SPNAME','VADD').show()

    # Q.23 Display count of two wheeler and four wheeler from ser_details
    # ser_det.groupBy('TYP_VEH').agg(count('*').alias('COUNT')).show()

    # Q24 Display name of customers who paid highest SPGST and for which item
    # customer.join(ser_det, on='CID', how='inner').groupBy('typ_ser','cname').agg(max('SP_GST').alias('max'))\
    #     .orderBy(col('max').desc()).show()

    # Q25 Display vendors name who have charged highest SPGST rate  for which item
    # SPGST = vendors.join(purchase, on='vid', how='inner').join(SparePart, on='spid', how='inner')
    # SPGST.groupBy('vname','spname').agg(max('sp_gst').alias('spgst')).orderBy(col('spgst').desc()).show(truncate=False)

    # Q26 list name of item and employee name who have received item
    # employee.join(purchase, employee.EID == purchase.RCV_EID, 'inner')\
    #     .join(SparePart, on='spid', how='inner').select('ENAME','SPNAME')\
    #     .show()

    # Q27 Display the Name and Vehicle Number of Customer who serviced his vehicle, And Name the Item used for Service,\
    # And specify the purchase date of that Item with his vendor and Item Unit and Location, And employee Name who \
    # serviced the vehicle. for Vehicle NUMBER "MH-14PA335".'

    # customer.join(ser_det, on='cid', how='inner').join(purchase, on='spid', how='inner')\
    #     .join(vendors, on='vid', how='inner').join(SparePart, on='spid', how='inner')\
    #     .join(employee, on='eid', how='inner').filter("VEH_NO=='MH14PA335'").show()

    # Q28 who belong this vehicle  MH-14PA335" Display the customer name
    # customer.join(ser_det, on='cid', how='inner').filter("VEH_NO=='MH14PA335'").show()

    # Q29 Display the name of customer who belongs to New York and when he /she service their  vehicle on which date
    # customer.join(ser_det, on='cid', how='inner').filter(col('cadd').like('NEW YORK'))\
    #     .select('cname','cadd','ser_date').show()

    # Q 30 from whom we have purchased items having maximum cost?
    # SparePart.join(purchase, on='spid', how='inner').groupBy('spname').agg(max('total').alias('max'))\
    #     .orderBy(col('max').desc()).show()

    # Q31 Display the names of employees who are not working as Mechanic and that employee done services.----
    # employee.join(ser_det, on='eid', how='inner').filter(col('E_JOB') != 'MECHANIC').show()

    # Q32 Display the various jobs along with total number of employees in each job. \
    # The output should contain only those jobs with more than two employees.
    # employee.groupBy('e_job').agg(count('eid').alias('eidCount')).orderBy(col('eidCount').desc()).show()

    # Q33 Display the details of employees who done service  and give them rank according to their no. of services .
    # employee.join(ser_det, on='eid', how='inner')\
    #     .withColumn('dense_rank', dense_rank().over(Window.orderBy(col('ser_qty').desc()))).show()

    # Q 34 Display those employees who are working as Painter and fitter \
    # and who provide service and total count of service done by fitter and painter
    # employee.join(ser_det, on='eid', how='inner').filter(trim(col('E_JOB')).isin('PAINTER', 'FITTER')).show()

    # Q35 Display employee salary and as per highest  salary provide Grade to employee
    # employee.withColumn('Grade', when(employee.E_SAL_ >= 2000, 'A')
    #                             .when(employee.E_SAL_ >= 1700, 'B')
    #                             .when(employee.E_SAL_ >= 1200, 'C')
    #                             .when(employee.E_SAL_ >= 1100, 'D')
    #                             .when(employee.E_SAL_ >= 1000, 'E')).show()

    # Q36  display the 4th record of emp table without using group by and rowid
    # employee.withColumn('row_number', row_number().over(Window.orderBy(col('eid')))).\
    # filter(col('row_number') == 4).show()

    # Q37 Provide a commission 100 to employees who are not earning any commission.
    # Comm = employee.join(ser_det, on='eid', how='inner').select(col('comm'))
    # Comm.withColumn('provide', when(Comm.comm == 0, 100)
    #                            .otherwise(Comm.comm)).show()

    # Q38 write a query that totals no. of services  for each day and place the results in descending order
    # ser_det.select('ser_date', 'ser_qty').orderBy(col('ser_qty').desc()).show()

    # Q39 Display the service details of those customer who belong from same city--
    # customer.join(ser_det, on='cid', how='full').filter(col('CADD') == col('CADD')).show()

    # Q40 write a query join customers table to itself to find all pairs of customers service by a single employee
    # customer.alias('c1').join(customer.alias('c2'), on=col('c1.cid') == col('c2.cid'))\
    #     .filter((col('c1.cid') < col('c2.cid')) & (col('c1.cid') == col('c2.cid'))).show()

    # Q41 List each service number follow by name of the customer who made  that service
    # customer.join(ser_det, on='cid', how='inner').select('cname', 'typ_ser').show(truncate=False)

    # Q42 Write a query to get details of employee and provide rating on basis of  maximum services provide by employee\
    # .Note (rating should be like A,B,C,D)
    # rating = employee.join(ser_det, on='eid', how='inner').select('ename','ser_qty').orderBy(col('ser_qty').desc())
    # rating.withColumn('Rating', when(rating.ser_qty == 2, 'A')
    #                           .when(rating.ser_qty == 1, 'B')
    #                           .otherwise(rating.ser_qty)).show()

    # Q43 Write a query to get maximum service amount of each customer with their customer details ?
    # customer.join(ser_det, on='cid', how='inner').groupBy('cid','cname','cadd')\
    #     .agg(max('ser_amt').alias('ser_amt')).orderBy(col('ser_amt').desc()).show()

    # Q44 Get the details of customers with his total no of services ?
    # customer.join(ser_det, on='cid', how='inner').select('cid', 'cname', 'cadd', 'C_C0NTACT', 'ser_qty')\
    #     .orderBy(col('ser_qty').desc()).show()

    # Q45 From which location sparpart purchased  with highest cost ?
    # SparePart.join(purchase, on='spid', how='inner').join(vendors, on='vid', how='inner')\
    #     .groupBy('VADD','SPNAME').agg(max('total').alias('total')).orderBy(col('total').desc()).show()

    # Q46 Get the details of employee with their service details who has salary is null
    # employee.join(ser_det, on='eid', how='full').filter('E_SAL_== 0').show()

    # Q47 find the sum of purchase location wise
    # Purchase = purchase.join(vendors, on='vid', how='inner').select('total', 'vadd')
    # Purchase.groupBy('vadd').agg(sum('total').alias('sum'))\
    #     .orderBy(col('sum').desc()).show()

    # Q48 write a query sum of purchase amount in word location wise ?
    # Purchase = purchase.join(vendors, on='vid', how='inner').select('total', 'vadd')
    # Word = Purchase.groupBy('vadd').agg(sum('total').alias('sum'))\
    #     .orderBy(col('sum').desc())
    # Word.createOrReplaceTempView('emp')
    # spark.sql("select vadd,sum from emp").show()

    # Q49 Has the customer who has spent the largest amount money has been give highest rating
    # Rating = customer.join(ser_det, on='cid', how='inner').groupBy('cname').agg(max('total').alias('total'))\
    #     .orderBy(col('total').desc())
    #
    # Rating.withColumn('store', when(Rating.total > 1400, 'A')
    #                            .when(Rating.total > 700, 'B')
    #                            .when(Rating.total > 400, 'C')
    #                            .when(Rating.total > 300, 'D')
    #                            .when(Rating.total > 100, 'E')
    #                            .otherwise(Rating.total)).show()

    # Q50 select the total amount in service for each customer for which the total is greater than the amount of the
    # largest service amount in the table

    # largest = customer.join(ser_det, on='cid', how='inner').groupBy('cid', 'cname')\
    # .agg(sum('ser_amt').alias('ser_amt'))
    # MAX = customer.join(ser_det, on='cid', how='inner').agg(max('ser_amt')).collect()[0][0]
    # store = largest.filter(largest['ser_amt'] > MAX)
    # store.show()

    # Q51  List the customer name and sparepart name used for their vehicle and  vehicle type
    # customer.join(ser_det, on='cid', how='inner').select('CNAME', 'TYP_VEH', 'TYP_SER').show(truncate=False)

    # Q52 Write a query to get spname ,ename,cname quantity ,rate ,service amount for record exist in service table
    # SparePart.join(ser_det, on='spid', how='inner').join(employee, on='eid', how='inner')\
    #     .join(customer, on='cid', how='inner').select('SPNAME','ENAME','CNAME','SER_AMT').show(truncate=False)

    # Q53 specify the vehicles owners who’s tube damaged.
    # customer.join(ser_det, on='cid', how='inner').filter(col('typ_ser').like('TUBE DAMAGED')).show()

    # Q.54 Specify the details who have taken full service.

    # Q.55 Select the employees who have not worked yet and left the job.
    # employee.join(ser_det, on='eid', how='leftAnti').show()

    # Q.56  Select employee who have worked first ever.
    # employee.join(ser_det, on='eid', how='inner').orderBy(col('E_DOJ').asc()).show()

    # Q.57 Display all records falling in odd date--
    # ser_det.filter((dayofmonth(col("SER_DATE").cast("date")) % 2 == 1)).show()

    # Q.58 Display all records falling in even date--
    # ser_det.filter((dayofmonth(col("SER_DATE").cast("date")) % 2 == 0)).show()

    # Q.59 Display the vendors whose material is not yet used.
    # SparePart.join(ser_det, on='spid', how='leftAnti').show (truncate=False)

    # Q.60 Difference between purchase date and used date of spare part.

    # spark session terminate
    spark.stop()
    #