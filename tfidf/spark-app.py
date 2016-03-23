#!/usr/bin/env python
# spark-app.py
# A Spark application that computes the TF-IDF of a corpus.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 14:16:57 2015 -0500

"""
A Spark application that computes the TF-IDF of a corpus.
"""

##########################################################################
## Imports
##########################################################################

import re
import math

from operator import add
from functools import partial
from pyspark import SparkConf, SparkContext

##########################################################################
## Global Variables
##########################################################################

APP_NAME  = "TF-IDF of Reuters Corpus"
N_DOCS    = 41

# Regular expressions
header    = re.compile(r'^=+\s(\d+)\s=+$', re.I)
tokenizer = re.compile(r'\W+')

##########################################################################
## Helper functions
##########################################################################

def chunk(args):
    """
    Splits a text file into smaller document id, text pairs.
    """

    def chunker(text):
        """
        Inner generator function.
        """
        docid    = None
        document = []
        for line in text.split("\n"):
            # Check to see if we are on a header line.
            hdr = header.match(line)
            if hdr is not None:
                # If we are on a current document, emit it.
                if docid is not None:
                    yield (docid, document)

                # If so, extract the document id and reset the document.
                docid = hdr.groups()[0]
                document = []
                continue
            else:
                document.append(line)

    fname, text = args
    return list(chunker(text))


def tokenize(document, stopwords=None):
    """
    Tokenize and return (docid, word) pairs with a counter.
    """

    def inner_tokenizer(lines):
        """
        Inner generator for word tokenization.
        """
        for line in lines:
            for word in re.split(tokenizer, line):
                if word and word not in stopwords.value and word.isalpha():
                    yield word

    docid, lines = document
    return [
        ((docid, word), 1) for word in inner_tokenizer(lines)
    ]


def term_frequency(v1, v2):
    """
    Compute the term frequency by splitting the complex value.
    """
    docid, tf, count1   = v1
    _docid, _tf, count2 = v2
    return (docid, tf, count1 + count2)


def tfidf(args):
    """
    Compute the TF-IDF given a ((word, docid), (tf, n)) argument.
    """
    (key, (tf, n)) = args
    if n > 0:
        idf = math.log(N_DOCS/n)
        return (key, idf*tf)


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

    # Load the corpus as whole test files and chunk them.
    corpus  = sc.wholeTextFiles('reuters.txt').flatMap(chunk)

    # Phase one: tokenize and sum (word, docid), count pairs (document frequency).
    docfreq = corpus.flatMap(partial(tokenize, stopwords=stopwords))
    docfreq = docfreq.reduceByKey(add)

    # Phase two: compute term frequency then perform keyspace change.
    trmfreq = docfreq.map(lambda (key, tf): (key[1], (key[0], tf, 1)))
    trmfreq = trmfreq.reduceByKey(term_frequency)
    trmfreq = trmfreq.map(lambda (word, (docid, tf, n)): ((word, docid), (tf, n)))

    # Phase three: comptue the tf-idf of each word, document pair.
    tfidfs  = trmfreq.map(tfidf)

    # Write the results out to disk
    tfidfs.saveAsTextFile("reuters-tfidfs")

if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
