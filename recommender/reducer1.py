#!/usr/bin/env python

from framework import Reducer
from ast import literal_eval as make_tuple

class ProductsReducer(Reducer):

    def reduce(self):
        for current, group in self:
            products = [item[1] for item in group]
            for producta in products:
                for productb in products:
                    if producta != productb:
                        key = [producta, productb]
                        key.sort()
                        self.emit(tuple(key), 1)

if __name__ == '__main__':
    reducer = ProductsReducer()
    reducer.reduce()
