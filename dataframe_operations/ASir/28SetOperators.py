from pyspark.sql import SparkSession


# Assignment No:04
# Assignment Description : union,union all,intersect(desc),intersectAll(asc),subtract(a-b desc),subtract(b-a asc)
# Date:2023-06-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Set Operators" the application name
    spark = SparkSession.builder.master('local[*]').appName('Set Operators').getOrCreate()

    # create emp table data
    emp_id = [
        (1, 'John', 'Sena', 'aus', 101),
        (2, 'alex', 'carry', 'ger', 102),
        (3, 'tim', 'sout', 'eng', 103),
        (4, 'sau', 'agar', 'ind', 105)
    ]

    #
    emp_id1 = [
        (1, 'John', 'Sena', 'aus', 101),
        (5, 'alex', 'carry', 'ger', 102),
        (5, 'tim', 'sout', 'eng', 103),
        (2, 'sau', 'agar', 'ind', 105)
    ]

    # create df on top of the emp table
    emp_df = spark.createDataFrame(emp_id, ['id', 'name', 'lastname', 'country', 'dept_id'])

    # create df on top of the dept table
    dept_df = spark.createDataFrame(emp_id1, ['id', 'name', 'lastname', 'country', 'dept_id'])

    # Union
    print('union')
    emp_df.union(dept_df).show()

    # Union all
    print('unionAll')
    emp_df.unionAll(dept_df).show()

    # union with use distinct
    print('union distinct')
    emp_df.union(dept_df).distinct().show()

    # intersect : show common records in both table,desc order
    print('intersect')
    emp_df.intersect(dept_df).show()

    # intersectAll: show common records in both table asc order
    print('intersectAll')
    emp_df.intersectAll(dept_df).show()

    # subtract: common records in emp_df
    print('subtract emp_df----dept_df')
    emp_df.subtract(dept_df).show()

    # subtract: common records in dept_df
    print('subtract dept_df----emp_df')
    dept_df.subtract(emp_df).orderBy('id').show()

    # Except:Returns the unique rows from the left SELECT statement after excluding the rows \
    # that appear in the right SELECT statement.
    print('exceptAll')
    emp_df.exceptAll(dept_df).show()

    # spark session close
    spark.stop()
    #