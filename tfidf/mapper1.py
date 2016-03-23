#!/usr/bin/env python
# mapper1.py
# Emits the term frequency of every term in each document.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 12:55:21 2015 -0500

"""
Emits the term frequency of every term in each document.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input reuters \
        -output reuters_term_frequency \
        -mapper mapper1.py \
        -reducer reducer1.py \
        -file mapper1.py \
        -file reducer1.py \
        -file stopwords.txt \
        -file framework.py

The output should be the frequency of term, document pairs.
"""

##########################################################################
## Imports
##########################################################################

import re
import sys

from framework import Mapper

##########################################################################
## Module Constants
##########################################################################

header = re.compile(r'^=+\s(\d+)\s=+$', re.I)

##########################################################################
## Mapping Functionality
##########################################################################

class TermFrequencyMapper(Mapper):

    def __init__(self, *args, **kwargs):
        """
        Initialize the tokenizer and stopwords.
        """
        super(TermFrequencyMapper, self).__init__(*args, **kwargs)

        self.stopwords = set()
        self.tokenizer = re.compile(r'\W+')
        self.current   = None

        # Read the stopwords from the text file.
        with open('stopwords.txt') as stopwords:
            for line in stopwords:
                self.stopwords.add(line.strip())

    def tokenize(self, text):
        """
        Tokenizes and normalizes a line of text (yields only non-stopwords
        that aren't digits, punctuation, or empty strings).
        """
        for word in re.split(self.tokenizer, text):
            if word and word not in self.stopwords and word.isalpha():
                yield word

    def map(self):
        for line in self:

            if header.match(line):
                # We have a new document! Get the document id:
                self.current = header.match(line).groups()[0]

            else:
                # Only emit words that have a document id.
                if self.current is None: continue

                # Otherwise tokenize the line and emit every (word, docid).
                for word in self.tokenize(line):
                    self.emit((word, self.current), 1)

if __name__ == '__main__':
    mapper = TermFrequencyMapper(sys.stdin)
    mapper.map()
