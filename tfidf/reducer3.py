#!/usr/bin/env python
# reducer3.py
# A simple implementation of the IdentityReducer.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 13:13:21 2015 -0500

"""
A simple implementation of the IdentityReducer.

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

The identity reducer is just a pass through for the mapper output.
"""

##########################################################################
## Imports
##########################################################################

import sys

from operator import itemgetter
from framework import Reducer

##########################################################################
## Reducing Functionality
##########################################################################

class IdentityReducer(Reducer):

    def reduce(self):
        for current, group in self:
            for item in group:
                self.emit(current, item[1])

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    reducer = IdentityReducer(sys.stdin)
    reducer.reduce()
