#!/usr/bin/env python
# spark-app.py
# A Spark application that computes the frequency of trigrams.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:

"""
A Spark application that computes the frequency of trigrams in a tokenized
and part of speech tagged corpus.
"""

##########################################################################
## Imports
##########################################################################

from operator import add
from functools import partial
from string import punctuation
from pyspark import SparkConf, SparkContext

##########################################################################
## Global Variables
##########################################################################

APP_NAME = "Trigram Frequency"

##########################################################################
## Helper functions
##########################################################################

def tokenize(line):
    """
    Normalizes the line (making it all lowercase) then tokenizes it, removing
    the part of speech tag.
    """
    words = line.lower().split()
    return [word.split("/")[0] for word in words]


def include(word, stopwords):
    """
    Returns True if the word is not an exclusion word, either punctuation or a
    stopword. Use this helper function in a filter.
    """
    return word not in punctuation and word not in stopwords.value


def ngrams(words, n):
    """
    Returns the NGrams by performing a sliding window over a list of words.
    """
    words = list(words) # Convert the generator to a list
    return zip(*[words[idx:] for idx in xrange(n)])

##########################################################################
## Primary Analysis and Main Method
##########################################################################

def main(sc):
    """
    Primary analysis mechanism for Spark application
    """

    # Load stopwords from the dataset
    with open('stopwords.txt', 'r') as words:
        stopwords = frozenset([
            word.strip() for word in words.read().split("\n")
        ])

    # Broadcast the stopwords across the cluster
    stopwords = sc.broadcast(stopwords)

    # Create an accumulator to count total number of trigrams and words.
    words     = sc.accumulator(0)
    trigrams  = sc.accumulator(0)

    # Load and parse the corpus from HDFS and insert into an RDD
    tokens = sc.textFile("textcorpus").flatMap(tokenize)

    # Perform the word count
    tokens.foreach(lambda w: words.add(1))

    # Filter stopwords and extract trigrams
    tokens = tokens.filter(partial(include, stopwords=stopwords))
    tokens = tokens.mapPartitions(partial(ngrams, n=3))

    # Perform the trigram count
    tokens.foreach(lambda ng: trigrams.add(1))

    # Counter per-trigram frequency
    tokens = tokens.map(lambda t: (t, 1)).reduceByKey(add)

    # Write output to disk
    tokens.saveAsTextFile("trigrams")

    print "Number of trigrams: {} in {} words.".format(trigrams.value, words.value)
    for trigram, frequency in tokens.sortBy(lambda (t,c): c).take(100):
        print "{}: {}".format(frequency, trigram)

if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
