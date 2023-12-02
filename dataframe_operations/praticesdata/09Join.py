from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('a').getOrCreate()
    data = [
        (1, 'Smith', -1, '2018', '10', 'M', 3000),
        (2, 'Rose', 1, '2010', '20', 'M', 4000),
        (3, 'Williams', 1, '2010', '10', 'M', 1000),
        (4, 'Jones', 2, '2005', '10', 'F', 2000),
        (5, 'Brown', 2, '2010', '40', '', -1),
        (6, 'Brown', 2, '2010', '50', '', -1)
    ]
    columns = ['emp_id', 'name', 'manager_id', 'joinYear', 'dept_id', 'gender', 'salary']

    empDf = spark.createDataFrame(data=data, schema=columns)


    dept = [
        ('Finance', 10),
        ('Marketing', 20),
        ('Sales', 30),
        ('IT', 40)
    ]
    columns = ['dept_name', 'dept_id']
    deptDf = spark.createDataFrame(data=dept, schema=columns)

# inner join dataFrame

    #   empDf.join(deptDf, on='dept_id', how='inner').show()
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'inner').show()

# fullOuter join : 1)full 2)outer 3)fullOuter

    #   empDf.join(deptDf, on='dept_id', how='full').show()
    #   empDf.join(deptDf, on='dept_id', how='outer').show()
    #   empDf.join(deptDf, on='dept_id', how='fullOuter').show()

# fullOuter join :1)full 2)outer 3)fullOuter

    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'full').show()
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'outer').show()
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'fullOuter').show()

#  left outer join 1)left 2)leftOuter

    #   empDf.join(deptDf, on='dept_id', how='left').show()
    #   empDf.join(deptDf, on='dept_id', how='leftOuter').show()

# left outer join 1)left 2)leftOuter
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'left').show()
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'leftOuter').show()

# right outer join 1)right 2)rightOuter
    #   empDf.join(deptDf, on='dept_id', how='right').show()
    #   empDf.join(deptDf, on='dept_id', how='rightOuter').show()

# right outer join 1)right 2)rightOuter
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'right').show()
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'rightOuter').show()

# leftSemi join: 1)leftSemi
    #   empDf.join(deptDf, on='dept_id', how='leftSemi').show()

# leftSemi join: 1)leftSemi
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'leftSemi').show()

# leftAnti join: 1)leftAnti
    #   empDf.join(deptDf, on='dept_id', how='leftAnti').show()
    #   empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'leftAnti').show()

# Using SQL expression
    empDf.createOrReplaceTempView('emp')
    deptDf.createOrReplaceTempView('dep')

    spark.sql('select * from emp e,dep d where e.dept_id == d.dept_id').show()
    spark.sql('select * from emp e inner join dep d on e.dept_id == d.dept_id').show()

    spark.stop()
