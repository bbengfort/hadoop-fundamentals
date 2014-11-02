#!/usr/bin/env python

import sys

from operator import itemgetter
from framework import Reducer

class IdentityReducer(Reducer):

    def reduce(self):
        for current, group in self:
            for item in group:
                self.emit(current, item[1])

if __name__ == '__main__':
    reducer = IdentityReducer(sys.stdin)
    reducer.reduce()
