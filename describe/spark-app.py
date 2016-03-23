#!/usr/bin/env python
# spark-app.py
# A Spark application that computes the summary statistics of arrival delays.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 15:13:56 2015 -0500

"""
A Spark application that computes the summary statistics of arrival delays.
"""

##########################################################################
## Imports
##########################################################################

import os
import sys
import csv
import math

from functools import partial
from StringIO import StringIO
from pyspark import SparkConf, SparkContext

##########################################################################
## Global Variables
##########################################################################

APP_NAME = "Summary Statistics of Arrival Delay by Airport"
DATASET  = os.path.join(os.path.dirname(__file__), "..", "data", "ontime", "flights.csv")

##########################################################################
## Helper functions
##########################################################################

def counters(line):
    """
    Splits the line on a CSV and parses it into the key and summary counters.
    A counter is as follows: (count, total, square, minimum, maximum).
    """
    reader = csv.reader(StringIO(line), delimiter=',')
    row = reader.next()

    airport = row[4]

    try:
        delay = float(row[8])
    except ValueError:
        delay = 0.0

    return (airport, (1, delay, delay ** 2, delay, delay))

def aggregation(item1, item2):
    """
    For an (airport, counters) item, perform summary aggregations.
    """
    count1, total1, squares1, min1, max1 = item1
    count2, total2, squares2, min2, max2 = item2

    minimum = min((min1, min2))
    maximum = max((max1, max2))
    count   = count1 + count2
    total   = total1 + total2
    squares = squares1 + squares2

    return (count, total, squares, minimum, maximum)

def summary(aggregate):
    """
    Compute summary statistics from aggregation.
    """
    (airport, (count, total, square, minimum, maximum)) = aggregate

    try:
        mean   = total / float(count)
        stddev = math.sqrt((square-(total**2)/count)/count-1)

        return (airport, (count, mean, stddev, minimum, maximum))
    except Exception:
        return (airport, (count, None, None, minimum, maximum))

##########################################################################
## Primary Analysis and Main Method
##########################################################################

def main(sc):
    """
    Primary analysis mechanism for Spark application
    """

    # Load data set and parse out statistical counters
    delays = sc.textFile(DATASET).map(counters)

    # Perform summary aggregation by key
    delays = delays.reduceByKey(aggregation)
    delays = delays.map(summary)

    # Write the results out to disk
    delays.saveAsTextFile("delays-summary")

if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
