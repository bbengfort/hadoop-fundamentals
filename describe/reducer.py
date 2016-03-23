#!/usr/bin/env python
# reducer.py
# Computes per-key statistical aggregations from a stats mapper.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 14:00:35 2015 -0500

"""
Computes per-key statistical aggregations from a stats mapper.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input ontime_flights.tsv \
        -output airport_delay_stats \
        -mapper mapper.py \
        -reducer reducer.py \
        -file mapper.py \
        -file reducer.py \
        -file framework.py

Outputs statistical aggregations per key from a striped mapper.
"""

##########################################################################
## Imports
##########################################################################

import sys
import math

from framework import Reducer
from ast import literal_eval as make_tuple

##########################################################################
## Reducing Functionality
##########################################################################

class StatsReducer(Reducer):

    def reduce(self):
        for key, values in self:

            count   = 0
            delay   = 0.0
            square  = 0.0
            minimum = None
            maximum = None

            for value in values:
                count  += value[0]
                delay  += value[1]
                square += value[2]

                if minimum is None or value[1] < minimum:
                    minimum = value[1]

                if maximum is None or value[1] > maximum:
                    maximum = value[1]

            mean   = delay / float(count)
            stddev = math.sqrt((square-(delay**2)/count)/count-1)

            self.emit(key, (count, mean, stddev, minimum, maximum))

    def __iter__(self):
        for current, group in super(StatsReducer, self).__iter__():
            yield current, map(make_tuple, [item[1] for item in group])

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    reducer = StatsReducer(infile=sys.stdin)
    reducer.reduce()
