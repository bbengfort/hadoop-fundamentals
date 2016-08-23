#!/usr/bin/env python

"""
A Mapper for counting words that uses iterators and generators for memory
efficiency as well as defines various helper functions.
"""

import sys

class Mapper(object):

    def __init__(self, infile=sys.stdin, separator='\t'):
        self.infile = infile
        self.sep    = separator

    def emit(self, key, value):
        sys.stdout.write("%s%s%s\n" % (key, self.sep, value))

    def map(self):
        for line in self:
            for word in line.split():
                self.emit(word, 1)

    def __iter__(self):
        for line in self.infile:
            yield line

if __name__ == "__main__":
    mapper = Mapper()
    mapper.map()
