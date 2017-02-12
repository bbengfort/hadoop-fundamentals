# Defines the spark word count job on arbitrary HDFS data. 
# To run: spark-submit wordcount.py

import os 
import sys
import argparse 

from operator import add 
from os import path as filepath
from pyspark import SparkConf, SparkContext


HDFS = "hdfs://{}".format(os.environ["HDFS"])
USER = filepath.join(HDFS, "user", "ec2-user")
APP_NAME = "Word Count"


def wc_job(sc, path, output, sort=False):
    # Create the input RDD 
    hdfs_path = filepath.join(USER, path)
    text = sc.textFile(hdfs_path) 

    # Do the wordcount job 
    counts = text.flatMap(lambda line: line.split())
    counts = counts.map(lambda word: (word, 1))
    counts = counts.reduceByKey(add)

    # Perform a sort by token if specified
    if sort:
        counts = counts.sortBy(lambda token: token[1], ascending=False)

    # Coalesce to a single file and save to disk 
    hdfs_output = filepath.join(USER, output)
    counts.coalesce(1).saveAsTextFile(hdfs_output)
    


if __name__ == "__main__":

    # Command line argument parsing
    parser = argparse.ArgumentParser(
        description="conduct an word count on data in the user dir"
    )

    parser.add_argument(
        'data', nargs=1, 
        help="specify the input path relative to the HDFS home directory",
    )

    parser.add_argument(
        'output', nargs=1, 
        help="specify the location on HDFS to write the output",
    )

    parser.add_argument(
        '-s', '--sort', action='store_true', default=False, 
        help='sort the words by count (expensive)' 
    )


    # Parse arguments and set spark environment
    args = parser.parse_args()
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)

    # Execute the WC job
    wc_job(sc, args.data[0], args.output[0], sort=args.sort)
