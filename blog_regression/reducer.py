#!/usr/bin/env python
# reducer
# A simple implementation of a MeanReducer.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Thu Nov 12 07:28:55 2015 -0500

"""
The mean reducer simply averages all of the values associated with a key.

To execute this reducer and associated mapper on Hadoop Streaming use the
following command:

    $ $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input input_dir \
        -output output_dir \
        -mapper mapper.py \
        -reducer reducer.py \
        -file mapper.py \
        -file reducer.py

The final result should be a mean by key for the emitted mapper.
"""

##########################################################################
## Imports
##########################################################################

from framework import Reducer

##########################################################################
## Reducing Functionality
##########################################################################

class MeanReducer(Reducer):

    def reduce(self):
        for key, values in self:
            count = 0
            total = 0.0

            for value in values:
                count += 1
                total += float(value[1])

            self.emit(key, (total / count))


##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    import sys

    reducer = MeanReducer(sys.stdin)
    reducer.reduce()
