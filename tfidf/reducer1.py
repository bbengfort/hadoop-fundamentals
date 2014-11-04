#!/usr/bin/env python

from framework import Reducer

class SumReducer(Reducer):

    def reduce(self):
        for current, group in self:
            self.emit(key, sum(int(item[1]) for item in values))

if __name__ == '__main__':
    reducer = SumReducer()
    reducer.reduce()
