#!/usr/bin/env python

import sys
import random
from math import sqrt
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS


##########################################################################
# Helpers
##########################################################################

def parse_rating(line, sep=','):
    """
    Parses a rating line
    Returns: tuple of (random integer, (user_id, profile_id, rating))
    """
    fields = line.strip().split(sep)
    user_id = int(fields[0])  # convert user_id to int
    profile_id = int(fields[1])  # convert profile_id to int
    rating = float(fields[2])  # convert rated_id to int
    return random.randint(1, 10), (user_id, profile_id, rating)


def parse_user(line, sep=','):
    """
    Parses a user line
    Returns: tuple of (user_id, gender)
    """
    fields = line.strip().split(sep)
    user_id = int(fields[0])  # convert user_id to int
    gender = fields[1]
    return user_id, gender


def compute_rmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error), or square root of the average value
        of (actual rating - predicted rating)^2
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictions_ratings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictions_ratings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


##########################################################################
# Main
##########################################################################

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Incorrect number of arguments, correct usage: dating_recommender.py [user_id] [match_gender]"
        sys.exit(-1)

    # Configure Spark
    conf = SparkConf().setMaster("local") \
                      .setAppName("Dating Recommender") \
                      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    matchseeker = int(sys.argv[1])
    gender_filter = sys.argv[2]

    # Create ratings RDD of (randint, (user_id, profile_id, rating))
    ratings = sc.textFile("/home/hadoop/hadoop-fundamentals/data/dating/ratings.dat") \
                 .map(parse_rating)

    # Create users RDD
    users = dict(sc.textFile("/home/hadoop/hadoop-fundamentals/data/dating/gender.dat") \
                   .map(parse_user) \
                   .collect())

    # Create the training (60%) and validation (40%) set, based on last digit
    # of timestamp
    num_partitions = 4
    training = ratings.filter(lambda x: x[0] < 6) \
                      .values() \
                      .repartition(num_partitions) \
                      .cache()

    validation = ratings.filter(lambda x: x[0] >= 6) \
                      .values() \
                      .repartition(num_partitions) \
                      .cache()

    num_training = training.count()
    num_validation = validation.count()

    print "Training: %d and validation: %d\n" % (num_training, num_validation)

    # rank is the number of latent factors in the model.
    # iterations is the number of iterations to run.
    # lambda specifies the regularization parameter in ALS
    rank = 8
    num_iterations = 8
    lmbda = 0.1

    # Train model with training data and configured rank and iterations
    model = ALS.train(training, rank, num_iterations, lmbda)

    # evaluate the trained model on the validation set
    print "The model was trained with rank = %d, lambda = %.1f, and %d iterations.\n" % \
        (rank, lmbda, num_iterations)

    # Print RMSE of model
    validation_rmse = compute_rmse(model, validation, num_validation)
    print "Its RMSE on the validation set is %f.\n" % validation_rmse

    # Filter on preferred gender
    partners = sc.parallelize([u[0] for u in filter(lambda u: u[1] == gender_filter, users.items())])

    # run predictions with trained model
    predictions = model.predictAll(partners.map(lambda x: (matchseeker, x))).collect()
    # sort the recommedations
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:10]

    print "Eligible partners recommended for User ID: %d" % matchseeker
    for i in xrange(len(recommendations)):
        print ("%2d: %s" % (i + 1, recommendations[i][1])).encode('ascii', 'ignore')

    # clean up
    sc.stop()
