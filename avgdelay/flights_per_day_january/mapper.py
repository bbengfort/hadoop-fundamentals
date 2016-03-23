#!/usr/bin/env python

import csv
import sys

SEP = "\t"


class Mapper(object):

    def __init__(self, stream, sep=SEP):
        self.stream = stream
        self.sep    = sep

    def emit(self, key, value):
        sys.stdout.write("%s%s%s\n" % (key, self.sep, value))

    def map(self):
        reader = csv.reader(self.stream, delimiter="\t")
        for row in reader:

            if len(row) > 0:
                self.emit(row[6], 1)

if __name__ == '__main__':
    mapper = Mapper(sys.stdin)
    mapper.map()
