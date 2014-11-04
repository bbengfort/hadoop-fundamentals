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
                word  = word.lower()
                docid = self.get_job_conf("map.input.file") or "DOCID1"
                if word and word not in self.stopwords and word.isalpha():
                    self.emit((word, docid), 1)

if __name__ == '__main__':
    mapper = TermFrequencyMapper(sys.stdin)
    mapper.map()
