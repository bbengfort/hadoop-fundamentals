import sys

import numpy as np

from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans


##########################################################################
# Helpers
##########################################################################

def parse_vector(line, sep=','):
    """
    Parses a line
    Returns: numpy array of the latitude and longitude
    """
    fields = line.strip().split(sep)
    latitude = float(fields[1])
    longitude = float(fields[2])
    return np.array([latitude, longitude])


##########################################################################
# Main
##########################################################################
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: kmeans <earthquake_csv> <k>"
        exit(-1)

    # Configure Spark
    conf = SparkConf().setMaster("local") \
                      .setAppName("Earthquake Clustering") \
                      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    # Create training RDD of (lat, long) vectors
    earthquakes_file = sys.argv[1]
    training = sc.textFile(earthquakes_file).map(parse_vector)

    k = int(sys.argv[2])

    # train model based on training data and k-clusters
    model = KMeans.train(training, k)

    print "Earthquake cluster centers: " + str(model.clusterCenters)
    sc.stop()
