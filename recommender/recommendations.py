#!/usr/bin/env python

from numpy import array
from pyspark.mllib.recommendation import ALS
from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Dating Recommendations")
    sc   = SparkContext(conf=conf)

    # Load and parse the Rating data
    data = sc.textFile("dating/ratings.csv")

    ratings = data.map(lambda line: array([float(x) for x in line.split(',')]))

    # Build the recommendation model using ALS
    rank = 100
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations)

    # Evaluate the model on training data
    testdata = ratings.map(lambda p: (int(p[0]), int(p[1])))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))

    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)

    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/ratesAndPreds.count()

    print("Mean Squared Error = " + str(MSE))
