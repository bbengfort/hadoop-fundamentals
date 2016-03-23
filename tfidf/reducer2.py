#!/usr/bin/env python
# reducer2.py
# A complex implementation of a SumReducer for a particular output type.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 13:04:43 2015 -0500

"""
A complex implementation of a SumReducer for a particular output type. This
also performs a key space change in prepration for the third phase mapper.

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

The final result should be a compound key for the word, document id as well
as the term frequency and the document term frequency sum.
"""

import sys

from framework import Reducer
from operator import itemgetter
from ast import literal_eval as make_tuple

##########################################################################
## Reducing Functionality
##########################################################################

class DocumentTermsReducer(Reducer):

    def reduce(self):
        for key, values in self:
            terms = sum(int(item[2]) for item in values)
            for docid, tf, num in values:
                self.emit((key, docid), (int(tf), terms))

    def __iter__(self):
        for current, group in super(DocumentTermsReducer, self).__iter__():
            yield current, map(make_tuple, [item[1] for item in group])

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    reducer = DocumentTermsReducer(sys.stdin)
    reducer.reduce()
