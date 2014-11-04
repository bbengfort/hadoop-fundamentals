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
	    if n > 0:
            	idf = math.log(self.N/n)
            	self.emit(key, idf*tf)

if __name__ == '__main__':
    mapper = TFIDFMapper(sys.stdin, documents=41)
    mapper.map()
