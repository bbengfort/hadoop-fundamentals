#!/usr/bin/env python

import sys
import math

from framework import Mapper
from ast import literal_eval as make_tuple

class TFIDFMapper(Mapper):

    def __init__(self, *args, **kwargs):
        self.N = kwargs.pop("documents")
        super(TFIDFMapper, self).__init__(*args, **kwargs)

    def map(self):
        for line in self:
            key, val = map(make_tuple, line.split(self.sep))
            tf, n = (int(x) for x in val)
            idf = math.log(self.N/n)
            self.emit(key, idf*tf)

if __name__ == '__main__':
    mapper = TFIDFMapper(sys.stdin, documents=2)
    mapper.map()

# def mapper((word, docid), (tf, n)):
#     # Assume the number of documents is known
#     # N is the number of documents in the corpus
#     idf = math.log(N/n)
#     emit((word, docid), idf*tf)

# def identityReducer(key, values):
#     pass
