#!/usr/bin/env python
# spark-app.py
# A Spark application that computes the MSE of a linear model on test data.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Thu Nov 12 07:29:58 2015 -0500

"""
A Spark application that computes the MSE of a linear model on test data.
Variant: uses Sckit-Learn to build a model from a sample training data set.
"""

##########################################################################
## Imports
##########################################################################

import os
import sys
import csv
import time
import numpy as np

from functools import partial
from StringIO import StringIO
from sklearn.linear_model import RidgeCV
from pyspark import SparkConf, SparkContext

##########################################################################
## Global Variables
##########################################################################

APP_NAME = "MSE of Blog Comments Regression"
DATADIR  = os.path.join(os.path.dirname(__file__), "..", "data", "blogData")
TRAINING = os.path.join(DATADIR, "blogData_train.csv")
TESTING  = os.path.join(DATADIR, "blogData_test*")
PARAMS   = os.path.join(os.path.dirname(__file__), "params.txt")

##########################################################################
## Helper functions
##########################################################################

def build(path):
    """
    Computes a linear regression using Ridge regularization.
    """
    print "Building the linear model using Ridge regression"
    start = time.time()

    # Load the data, the target is the last column.
    data  = np.loadtxt(path, delimiter=',')
    y = data[:,-1]
    X = data[:,0:-1]

    # Instantiate and fit the model.
    model = RidgeCV()
    model.fit(X, y)

    print "Finished training the linear model in {:0.3f} seconds".format(time.time() - start)
    return model

def parse(line):
    """
    Splits the line on a CSV and parses it into floats. Returns a tuple of:
    (X, y) where X is the vector of independent variables and y is the target
    (dependent) variable; in this case the last item in the row.
    """
    reader = csv.reader(StringIO(line))
    row = [float(x) for x in reader.next()]
    return (tuple(row[:-1]), row[-1])


def cost(row, model):
    """
    Computes the square error given the row.
    """
    X, y = row # extract the dependent and independent vals from the tuple.

    # Compute the predicted value based on the linear model
    yhat = model.value.predict([X])

    # Compute the square error of the prediction
    return (y - yhat) ** 2


##########################################################################
## Primary Analysis and Main Method
##########################################################################

def main(sc):
    """
    Primary analysis mechanism for Spark application
    """

    # Build the model locally on sampled data
    model = build(TRAINING)

    # Broadcast the model across the Spark cluster
    model = sc.broadcast(model)

    # Create an accumulator to sum the squared error
    sum_square_error = sc.accumulator(0)

    # Load and parse the blog data from HDFS and insert into an RDD
    blogs = sc.textFile(TESTING).map(parse)

    # Map the cost function and accumulate the sum.
    error = blogs.map(partial(cost, model=model))
    error.foreach(lambda cost: sum_square_error.add(cost))

    # Print and compute the mean.
    print "\n\nMSE: {}\n\n".format(sum_square_error.value / error.count())


if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
