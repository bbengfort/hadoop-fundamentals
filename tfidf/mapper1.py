#!/usr/bin/env python

import re
import sys

from framework import Mapper

class TermFrequencyMapper(Mapper):

    def __init__(self, *args, **kwargs):
        super(TermFrequencyMapper, self).__init__(*args, **kwargs)

        self.stopwords = set()
        self.tokenizer = re.compile(r'\W+')
        with open('stopwords.txt') as stopwords:
            for line in stopwords:
                self.stopwords.add(line.strip())

    def map(self):
        for line in self:
            for word in re.split(self.tokenizer, line):
                word = word.lower()
                if word and word not in self.stopwords and word.isalpha():
                    self.emit((word, "DOC1"), 1)

if __name__ == '__main__':
    mapper = TermFrequencyMapper(sys.stdin)
    mapper.map()



# import re
# tokenize = re.compile(r'\W+')

# def mapper(docid, line):
#     for word in re.split(tokenize, line):
#         emit((word, docid), 1)

# def reducer((word, docid), counts):
#     emit((word, docid), sum(counts))

# combiner = reducer
