from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.master('local[*]').appName('Random Sample').getOrCreate()

# Using Fraction to get a random sample
    df = spark.read.csv(r'C:\PYSPARK\04CSVFiles\homes.csv', header=True)
    df.sample(fraction=0.1).show()

# using seed to reproduce the same samples in pyspark
    df.sample(fraction=0.1, seed=12).show()

# Sample withReplacement(may contain duplicates)
    df.sample(True, fraction=0.2, seed=25).show()

# sampleBy() Syntax
    #   df.sampleBy(df.Rooms, fractions=0.2, seed=None).show()

# Pyspark RDD: RDD sample () Syntax & Example
    rdd = df.rdd.map(lambda x: (x[:]))
    rdd1 = rdd.sample(withReplacement=False, fraction=0.1, seed=None)

    for element in rdd1.collect():
        print(element)
    print('---------------------------------------------------------')
# RDD takeSample()
    rdd2 = df.rdd.map(lambda x: (x[:]))
    rdd3 = rdd2.takeSample(withReplacement=False, num=5, seed=None)

    for elementOne in rdd3:
        print(elementOne)


    spark.stop()