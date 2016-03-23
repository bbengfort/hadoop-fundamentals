#!/usr/bin/env python


from collections import namedtuple
from pyspark import SparkConf, SparkContext

##########################################################################
# Parsing Utilities
##########################################################################

MOVIE_FIELDS = ['movie_id', 'title']

USER_FIELDS = ['user_id', 'age', 'gender']

RATING_FIELDS = ['user_id', 'movie_id', 'rating', 'timestamp']

Movie = namedtuple('Movie', MOVIE_FIELDS)
User = namedtuple('User', USER_FIELDS)
Rating = namedtuple('Rating', RATING_FIELDS)


def parse_movie(row):
    """
    Parses a movie row and returns a Movie named tuple.
    """
    return Movie(*row[:2])

def parse_user(row):
    """
    Parses a user row and returns a User named tuple.
    """
    row[1] = int(row[1])  # convert age to int
    return User(*row[:3])

def parse_rating(row):
    """
    Parses a rating row and returns a Rating named tuple.
    """
    row[2] = float(row[2])  # convert rating to float
    return Rating(*row[:4])


##########################################################################
# Main
##########################################################################

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setMaster("local").setAppName("Gen-Y Movie Ratings")
    sc = SparkContext(conf=conf)

    # Create movies RDD
    movies = sc.textFile("/home/hadoop/ml-100k/u.item") \
                .map(lambda x: x.split('|')) \
                .map(parse_movie)

    # Convert to pair RDD of (movie_id, title) key-values
    movie_pairs = movies.map(lambda m: (m.movie_id, m.title))

    # Create gen_y RDD and filter on 18-24 age group, collect only user_ids
    gen_y = sc.textFile("/home/hadoop/ml-100k/u.user") \
                .map(lambda x: x.split('|')) \
                .map(parse_user) \
                .filter(lambda u: u.age >= 18 and u.age <= 24) \
                .map(lambda u: u.user_id) \
                .collect()

    # Broadcast gen_y to cache lookup
    gen_y_ids = sc.broadcast(set(gen_y))

    # Create ratings RDD
    ratings = sc.textFile("/home/hadoop/ml-100k/u.data") \
                .map(lambda x: x.split('\t')) \
                .map(parse_rating)

    # Filter ratings on gen_y users
    gen_y_ratings = ratings.filter(lambda r: r.user_id in gen_y_ids.value)

    # Convert ratings to a pair RDD of (movie_id, rating)
    rating_pairs = gen_y_ratings.map(lambda r: (r.movie_id, r.rating))

    # Compute average ratings
    rating_sum_count = rating_pairs.combineByKey(lambda value: (value, 1),
                                                 lambda x, value: (x[0] + value, x[1] + 1),
                                                 lambda x, y: (x[0] + y[0], x[1] + y[1]))
    rating_avg = rating_sum_count.map(lambda (movie_id, (value_sum, count)): (movie_id, value_sum / count))

    # Join movie and rating data on movie_id
    movie_rating_avg = movie_pairs.join(rating_avg)

    # Retain title and avg rating
    movie_ratings = movie_rating_avg.map(lambda (movie_id, title_rating): (title_rating[0], title_rating[1]))

    # Swap key/value and sort and re-swap
    sorted_ratings = movie_ratings.map(lambda (x, y): (y, x)).sortByKey(False).map(lambda (x, y): (y, x))

    sorted_ratings.coalesce(1).saveAsTextFile("/home/hadoop/gen_y_ratings")
