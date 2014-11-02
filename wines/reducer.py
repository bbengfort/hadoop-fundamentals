#!/usr/bin/env python

import sys
import json

from operator import itemgetter
from framework import Reducer

class SumReducer(Reducer):

    def reduce(self):
        for key, values in self:
            self.emit(key, sum(float(item[1]) for item in values))

if __name__ == '__main__':
    reducer = SumReducer(sys.stdin)
    reducer.reduce()
