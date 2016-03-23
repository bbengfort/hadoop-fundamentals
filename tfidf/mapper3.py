#!/usr/bin/env python
# mapper3.py
# Compute the TF-IDF for each term and document pair.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 13:08:46 2015 -0500

"""
Compute the TF-IDF for each term and document pair.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input reuters_document_term_frequency \
        -output reuters_tfidf \
        -mapper mapper3.py \
        -reducer reducer3.py \
        -file mapper3.py \
        -file reducer3.py \
        -file framework.py

The output should be the (word, docid) pair as a key and the TF-IDF as a
value. This is the final computation, but is a map-only job because of the
way that the second step output keys.

Note that the number of documents needs to be passed into the Mapper for this
to work since it wasn't computed ahead of time, and is really a constant.
"""

##########################################################################
## Imports
##########################################################################

import sys
import math

from framework import Mapper
from ast import literal_eval as make_tuple

##########################################################################
## Mapping Functionality
##########################################################################

class TFIDFMapper(Mapper):

    def __init__(self, *args, **kwargs):
        self.N = kwargs.pop("documents")
        super(TFIDFMapper, self).__init__(*args, **kwargs)

    def map(self):
        for line in self:
            key, val = map(make_tuple, line.split(self.sep))
            tf, n = (int(x) for x in val)
        if n > 0:
            idf = math.log(self.N/n)
            self.emit(key, idf*tf)

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    mapper = TFIDFMapper(sys.stdin, documents=41)
    mapper.map()
