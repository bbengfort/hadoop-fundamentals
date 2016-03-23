#!/usr/bin/env python
# reducer
# A simple implementation of a SumReducer.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Thu Nov 12 07:28:16 2015 -0500

"""
The sum reducer simply adds all of the values associated with a key.

To execute this reducer and associated mapper on Hadoop Streaming use the
following command:

    $ $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input input_dir \
        -output output_dir \
        -mapper mapper.py \
        -reducer reducer.py \
        -file mapper.py \
        -file reducer.py

The final result should be a sum by key for the emitted mapper.
"""

##########################################################################
## Imports
##########################################################################

from framework import Reducer

##########################################################################
## Reducing Functionality
##########################################################################

class SumReducer(Reducer):

    def reduce(self):
        for key, values in self:
            total = sum(int(count[1]) for count in values)
            self.emit(key, total)

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    import sys

    reducer = SumReducer(sys.stdin)
    reducer.reduce()
