from pyspark.sql import SparkSession

# Assignment No:05
# Assignment Description : unionByName,unionByName with allowMissingColumns
# Date:2023-07-07

if __name__ == '__main__':

    # create SparkSession with local[*] as the master and "Set Operators" the application name
    spark = SparkSession.builder.master('local[*]').appName('unionByName').getOrCreate()

    # unionByName use

    student = [
        (1, 'shiv', 'ltr', 100),
        (2, 'sau', 'klm', 101),
        (3, 'sam', 'klm', 102),
        (4, 'suji', 'nan', 103)
    ]
    # unionByName use also allowMissingColumns

    teacher = [
        (1, 'ltr', 'dp', 100),
        (2, 'nag', 'kp', 101),
        (3, 'ahm', 'gp', 102),
        (4, 'del', 'sa', 103)
    ]
    # unionByName use also allowMissingColumns

    teacher1 = [
        ('ab', 'delhi', 100),
        ('ca', 'mumbai', 102),
        ('sn', 'chennai', 103),
        ('gp', 'pune', 104)
    ]

    # create df with top of student data
    stuDf = spark.createDataFrame(student, ['id', 'name', 'city', 'dept_id'])

    # create df with top of teacher data
    teacherDf = spark.createDataFrame(teacher, ['id', 'city', 'name', 'dept_id'])

    # create df with top of teacher1 data
    teacher1Df = spark.createDataFrame(teacher1, ['name', 'city', 'dept_id'])

    # unionByName
    stuDf.unionByName(teacherDf).show()

    # unionByName
    teacherDf.unionByName(stuDf).show()

    # unionByName with allowMissingColumns
    # teacherDf.unionByName(teacher1Df, allowMissingColumns=True).show()

    # unionByName with allowMissingColumns
    # teacher1Df.unionByName(teacherDf, allowMissingColumns=True).show()

    # spark session close
    spark.stop()

    #