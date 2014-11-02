#!/usr/bin/env python

import sys

from framework import Reducer
from operator import itemgetter
from ast import literal_eval as make_tuple

class DocumentTermsReducer(Reducer):

    def reduce(self):
        for key, values in self:
            terms = sum(int(item[2]) for item in values)
            for docid, tf, num in values:
                self.emit((key, docid), (int(tf), terms))

    def __iter__(self):
        for current, group in super(DocumentTermsReducer, self).__iter__():
            yield current, map(make_tuple, [item[1] for item in group])

if __name__ == '__main__':
    reducer = DocumentTermsReducer(sys.stdin)
    reducer.reduce()

# def reducer(word, values):
#     terms = sum(num for (docid, tf, num) in values)
#     for docid, tf, num in values:
#         emit((word, docid), (tf, terms))
