from pyspark import SparkConf, SparkContext
import sys


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Incorrect number of arguments, correct usage: wordcount.py [inputfile] [outputfile]"
        sys.exit(-1)

    # set input and dictionary from args
    input = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setMaster("local").setAppName("Word Count")
    sc = SparkContext(conf=conf)

    sotu = sc.textFile(input)

    counts = sotu.flatMap(lambda line: line.split(" ")) \
                 .map(lambda word: (word, 1)) \
                 .reduceByKey(lambda a, b: a + b)

    counts.coalesce(1).saveAsTextFile(output)

    sc.stop()
    print "Done!"
