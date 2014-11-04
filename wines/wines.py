## Spark Application for performing SGD regression on wines.

import csv

from numpy import array
from StringIO import StringIO

from pyspark import SparkConf, SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD

# Load and parse the data
def parsePoint(line):
    values = csv.reader(StringIO(line), delimiter=";").next() # CSV parsing of line
    values = [float(x) for x in values]                       # Cast to all floats
    return LabeledPoint(values[-1], values[:-1])              # y = quality, X = row[:-1]

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Wine Regression")
    sc   = SparkContext(conf=conf)

    wines = sc.textFile("winequality-red.csv")
    parsedData = wines.map(parsePoint)

    # Build the model
    model = LinearRegressionWithSGD.train(parsedData)

    # Evaluate the model on training data
    valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y).count() / valuesAndPreds.count()
    print("Mean Squared Error = " + str(MSE))
