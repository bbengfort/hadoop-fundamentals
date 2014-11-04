#!/usr/bin/env python

from framework import Reducer

class HalfSumReducer(Reducer):

    def reduce(self):
        for key, values in self:
            self.emit(key, (sum(int(item[1]) for item in values)/2))

if __name__ == '__main__':
    reducer = HalfSumReducer()
    reducer.reduce()
