#!/usr/bin/env python

import sys

from ast import literal_eval as make_tuple
from framework import Mapper

class DocumentTermsMapper(Mapper):

    def map(self):
        for line in self:
            key, tf = line.split(self.sep)  # Split line into key, val parts
            tf = int(tf)                    # Ensure the term frequency is an int
            word, docid = make_tuple(key)   # Parse the tuple string
            self.emit(word, (docid, tf, 1))

if __name__ == '__main__':
    mapper = DocumentTermsMapper(sys.stdin)
    mapper.map()
