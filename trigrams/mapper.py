#!/usr/bin/env python
# mapper.py
# A mapper that emits trigrams from text with a counter value.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Thu Nov 12 12:16:16 2015 -0500

"""
A mapper that emits trigrams from text with a counter value.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input textcorpus \
        -output trigrams \
        -mapper mapper.py \
        -reducer reducer.py \
        -file mapper.py \
        -file reducer.py \
        -file stopwords.txt

The final result should be the frequency of trigrams in the corpus.
Note: This version does not use NLTK (unlike the reading).
"""

##########################################################################
## Imports
##########################################################################

import re
import sys
import string

from framework import Mapper

##########################################################################
## Compile regex for splitting without punctuation
##########################################################################

wordpunct_tokenize = re.compile(r'[\s{}]+'.format(re.escape(string.punctuation)))

##########################################################################
## Mapping Functionality
##########################################################################

class TrigramMapper(Mapper):

    def __init__(self, infile=sys.stdin, separator='\t'):
        super(TrigramMapper, self).__init__(infile, separator)

        # Load the stopwords from the associated stopwords file
        with open('stopwords.txt', 'r') as words:
            self.stopwords = frozenset([
                word.strip() for word in words.read().split("\n")
            ])

    def exclude(self, token):
        """
        Do not allow punctuation or stopwords in trigrams.
        """
        return (
            token in self.stopwords or
            token in string.punctuation
        )

    def normalize(self, token):
        """
        Any type of normalization is allowed, here we simply lowercase. Note
        that the text corpus is already split and tagged, so normalization
        also removes the part of speech tag.
        """
        return token.lower().split("/")[0]

    def tokenize(self, value):
        """
        Entry point for tokenization, normalization, and exclusion.
        """
        for token in value.split():
            token = self.normalize(token)
            if not self.exclude(token):
                yield token

    def trigrams(self, words):
        """
        Emits all trigrams from a list of words.
        """
        words = list(words)  # force generator to iterate
        return zip(*[words[idx:] for idx in xrange(3)])

    def map(self):
        for value in self:
            for trigram in self.trigrams(self.tokenize(value)):
                self.counter("ngrams") # Count the total number of trigrams
                self.emit(trigram, 1)   # Emit the trigram and a frequency to sum

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    mapper = TrigramMapper()
    mapper.map()
