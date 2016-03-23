#!/usr/bin/env python
# mapper2.py
# Key space and value space change in preparation for document frequency.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 12:55:21 2015 -0500

"""
Key space and value space change in preparation for document frequency.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input reuters_term_frequency \
        -output reuters_document_term_frequency \
        -mapper mapper2.py \
        -reducer reducer2.py \
        -file mapper2.py \
        -file reducer2.py \
        -file framework.py

The output should be the word key and association of documents, keeping the
term frequency for use later in the algorithm.
"""

##########################################################################
## Imports
##########################################################################

import sys

from ast import literal_eval as make_tuple
from framework import Mapper

##########################################################################
## Mapping Functionality
##########################################################################

class DocumentTermsMapper(Mapper):

    def map(self):
        for line in self:
            key, tf = line.split(self.sep)  # Split line into key, val parts
            tf = int(tf)                    # Ensure the term frequency is an int
            word, docid = make_tuple(key)   # Parse the tuple string
            self.emit(word, (docid, tf, 1))

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    mapper = DocumentTermsMapper(sys.stdin)
    mapper.map()
