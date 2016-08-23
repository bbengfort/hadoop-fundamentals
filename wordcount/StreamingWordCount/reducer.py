#!/usr/bin/env python

"""
A Reducer in Python that is memory efficient by using iterators.
"""

import sys
from itertools import groupby
from operator import itemgetter

class Reducer(object):

    def __init__(self, infile=sys.stdin, separator="\t"):
        self.infile = infile
        self.sep    = separator

    def emit(self, key, value):
        sys.stdout.write("%s%s%s\n" % (key, self.sep, value))

    def reduce(self):
        for current, group in groupby(self, itemgetter(0)):
            try:
                total = sum(int(count) for current, count in group)
                self.emit(current, total)
            except ValueError:
                pass

    def __iter__(self):
        for line in self.infile:
            yield line.rstrip().split(self.sep, 1)

if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce()
