#!/usr/bin/env python
# mapper.py
# Mapper that emits the error of a linear model computed on a row of test data.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Thu Nov 12 06:57:16 2015 -0500

"""
Mapper that emits the MSE of a linear model computed on a row of test data.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input blogData \
        -output blog_mse \
        -mapper mapper.py \
        -reducer reducer.py \
        -file mapper.py \
        -file reducer.py \
        -file params.txt

The final result should be the total mean squared error for the test set.
"""

##########################################################################
## Imports
##########################################################################

import sys
import csv

from framework import Mapper

##########################################################################
## Mapping Functionality
##########################################################################

class MSEMapper(Mapper):

    def __init__(self, *args, **kwargs):
        """
        On initialization, load the coefficients and intercept of the linear
        model for use in downstream processing of the MSE.
        """
        super(MSEMapper, self).__init__(*args, **kwargs)

        # Store required computation
        self.coef = []
        self.intercept = None

        # Load the parameters from the text file
        with open('params.txt', 'r') as params:
            # Read the file and split on new lines
            data = params.read().split("\n")

            # Parse the data into floats
            data = [float(row.strip()) for row in data if row.strip()]

            # Everything but the last value are the thetas (coefficients)
            self.coef = data[:-1]

            # The last value is the intercept
            self.intercept = data[-1]

    def read(self):
        """
        Adapt the read function to be a CSV parser, since this is CSV data.
        """
        reader = csv.reader(super(MSEMapper, self).read())
        for row in reader:
            # Parse the row into floats
            yield [float(x) for x in row]

    def compute_error(self, row):
        vals = row[:-1] # The dependent variables from the row
        y    = row[-1]  # The independent variable is the last item in the row

        # Compute the predicted value based on the linear model
        yhat = sum([b*x for (b,x) in zip(self.coef, vals)]) + self.intercept

        # Compute the square error of the prediction
        return (y - yhat) ** 2

    def map(self):
        """
        The mapper will compute the MSE for each row in the data set and emit
        it with a "dummy key" that is, we'll compute the total for the entire
        data set, so we don't need to group by a key for multiple reducers.
        """
        for row in self:
            self.emit(1, self.compute_error(row))


##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    mapper = MSEMapper(sys.stdin)
    mapper.map()
