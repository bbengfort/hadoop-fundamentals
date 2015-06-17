#!/usr/bin/env python

import sys
from math import sqrt
from operator import add
from os.path import isfile

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

##########################################################################
# Helpers
##########################################################################

def parse_rating(line, sep='::'):
    """
    Parses a rating line
    Returns: tuple of (timestamp, (user_id, movie_id, rating))
    """
    fields = line.strip().split(sep)
    user_id = int(fields[0])  # convert user_id to int
    movie_id = int(fields[1])  # convert movie_id to int
    rating = float(fields[2])  # convert rating to float
    last_digit = long(fields[3]) % 10  # convert to last digit of timestamp
    return last_digit, (user_id, movie_id, rating)

def parse_movie(line, sep='::'):
    """
    Parses a movie line
    Returns: tuple of (movie_id, title)
    """
    fields = line.strip().split(sep)
    movie_id = int(fields[0])  # convert movie_id to int
    title = fields[1]
    return movie_id, title

def load_ratings(file, sep='\t'):
    """
    Load ratings from file
    """
    if not isfile(file):
        print "File %s does not exist." % file
        sys.exit(1)
    f = open(file, 'r')
    # Filter based on movie ratings that have been seen (0 = not seen)
    ratings = filter(lambda r: r[2] > 0, [parse_rating(line, '\t')[1] for line in f])
    f.close()
    if not ratings:
        print "No ratings provided."
        sys.exit(1)
    else:
        return ratings

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
    if len(sys.argv) != 2:
        print "Incorrect number of arguments, correct usage: movie_recommender.py [ratingsfile]"
        sys.exit(-1)

    # Configure Spark
    conf = SparkConf().setMaster("local") \
                      .setAppName("Movie Recommender") \
                      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    # set ratings_file to argument
    ratings_file = sys.argv[1]

    # load personal ratings as array of rating tuples (filter out 0 ratings)
    my_ratings = load_ratings(ratings_file)
    my_ratings_rdd = sc.parallelize(my_ratings, 1)

    # Create ratings RDD of (last digit of timestamp, (user_id, movie_id, rating))
    ratings = sc.textFile("/home/hadoop/hadoop-fundamentals/data/ml-1m/ratings.dat") \
                 .map(parse_rating)

    # Create movies RDD, collect, and convert to dict of {movie_id: title}
    movies = dict(sc.textFile("/home/hadoop/hadoop-fundamentals/data/ml-1m/movies.dat")
                   .map(parse_movie)
                   .collect())

    # Compute total ratings, and unique users and movies for statistics info
    num_ratings = ratings.count()
    num_users = ratings.values().map(lambda r: r[0]).distinct().count()
    num_movies = ratings.values().map(lambda r: r[1]).distinct().count()

    print "Got %d ratings from %d users on %d movies.\n" % (num_ratings, num_users, num_movies)

    # Create the training (60%) and validation (40%) set, based on last digit
    # of timestamp
    num_partitions = 4
    training = ratings.filter(lambda x: x[0] < 6) \
                      .values() \
                      .union(my_ratings_rdd) \
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
    rank = 12
    num_iterations = 10
    lmbda = 0.1

    # Train model with training data and configured rank and iterations
    model = ALS.train(training, rank, num_iterations, lmbda)

    # Print RMSE of model
    validation_rmse = compute_rmse(model, validation, num_validation)

    # evaluate the trained model on the validation set
    print "The model was trained with rank = %d, lambda = %.1f, and %d iterations.\n" % \
        (rank, lmbda, num_iterations)
    print "Its RMSE on the validation set is %f.\n" % validation_rmse

    # get set of movie_ids from my_ratings
    my_rated_movie_ids = set([r[1] for r in my_ratings])
    # get movies not seen/rated
    candidates = sc.parallelize([m for m in movies if m not in my_rated_movie_ids])
    # run predictions with trained model
    predictions = model.predictAll(candidates.map(lambda x: (0, x))).collect()
    # sort the recommedations
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]

    print "Movies recommended for you:"
    for i in xrange(len(recommendations)):
        print ("%2d: %s" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')

    # clean up
    sc.stop()
