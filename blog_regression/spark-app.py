#!/usr/bin/env python
# spark-app.py
# A Spark application that computes the MSE of a linear model on test data.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Thu Nov 12 07:29:58 2015 -0500

"""
A Spark application that computes the MSE of a linear model on test data.
"""

##########################################################################
## Imports
##########################################################################

import os
import sys
import csv

from functools import partial
from StringIO import StringIO
from pyspark import SparkConf, SparkContext

##########################################################################
## Global Variables
##########################################################################

APP_NAME = "MSE of Blog Comments Regression"
DATASET  = os.path.join(os.path.dirname(__file__), "..", "data", "blogData")
PARAMS   = os.path.join(os.path.dirname(__file__), "params.txt")

##########################################################################
## Helper functions
##########################################################################

def parse(line):
    """
    Splits the line on a CSV and parses it into floats. Returns a tuple of:
    (X, y) where X is the vector of independent variables and y is the target
    (dependent) variable; in this case the last item in the row.
    """
    reader = csv.reader(StringIO(line))
    row = [float(x) for x in reader.next()]
    return (tuple(row[:-1]), row[-1])


def cost(row, coef, intercept):
    """
    Computes the square error given the row.
    """
    X, y = row # extract the dependent and independent vals from the tuple.

    # Compute the predicted value based on the linear model
    yhat = sum([b*x for (b,x) in zip(coef.value, X)]) + intercept.value

    # Compute the square error of the prediction
    return (y - yhat) ** 2


##########################################################################
## Primary Analysis and Main Method
##########################################################################

def main(sc):
    """
    Primary analysis mechanism for Spark application
    """

    # Load coefficients and intercept from local file
    coef = []
    intercept = None

    # Load the parameters from the text file
    with open(PARAMS, 'r') as params:
        # Read the file and split on new lines and parse into floats
        data = [
            float(row.strip())
            for row in params.read().split("\n")
            if row.strip()
        ]

        coef = data[:-1]        # Everything but the last value are the thetas (coefficients)
        intercept = data[-1]    # The last value is the intercept

    # Broadcast the parameters across the Spark cluster
    # Note that this data is small enough you could have used a closure
    coef      = sc.broadcast(coef)
    intercept = sc.broadcast(intercept)

    # Create an accumulator to sum the squared error
    sum_square_error = sc.accumulator(0)

    # Load and parse the blog data from HDFS and insert into an RDD
    blogs = sc.textFile(DATASET).map(parse)

    # Map the cost function and accumulate the sum.
    error = blogs.map(partial(cost, coef=coef, intercept=intercept))
    error.foreach(lambda cost: sum_square_error.add(cost))

    # Print and compute the mean.
    print "\n\nMSE: {}\n\n".format(sum_square_error.value / error.count())


if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
