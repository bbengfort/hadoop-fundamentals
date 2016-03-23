#!/usr/bin/env python
# mapper.py
# Maps values that are used in a stripes-based statistical aggregation.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 13:40:05 2015 -0500

"""
Maps values that are used in a stripes-based statistical aggregation.

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

Outputs a key with (count, delay, square, latest minimum, and latest maximum).
"""

##########################################################################
## Imports
##########################################################################

import csv
import sys

from framework import Mapper

##########################################################################
## Mapping Functionality
##########################################################################

class DelayStatsMapper(Mapper):

    def __init__(self, delimiter="\t", quotechar='"', **kwargs):
        super(DelayStatsMapper, self).__init__(**kwargs)
        self.delimiter = delimiter
        self.quotechar = quotechar

    def map(self):
        for row in self:
            try:
                airport = row[6]
                delay   = float(row[15])
            except ValueError:
                # Could not parse the delay, which is zero.
                delay   = 0.0

            self.emit(airport, (1, delay, delay ** 2))

    def read(self):
        """
        Parse the tab delimited flights dataset with the CSV module.
        """
        reader = csv.reader(self.infile, delimiter=self.delimiter)
        for row in reader:
            yield row

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    mapper = DelayStatsMapper(infile=sys.stdin)
    mapper.map()
