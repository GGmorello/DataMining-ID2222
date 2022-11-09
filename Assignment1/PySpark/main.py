from pyspark.sql import SparkSession

def main():
    dir = "docs"  # Should be some file on your system
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    rddWhole = spark.sparkContext.wholeTextFiles(dir).cache()
    shingleSize = 5
    allShingles = {}

    for f in rddWhole:
        for i in range(len(f) - shingleSize + 1):
            print(f[i: i + shingleSize])

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

    spark.stop()

if __name__ == '__main__':
    main()