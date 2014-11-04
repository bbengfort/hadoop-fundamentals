#!/usr/bin/env python

import csv
from framework import Mapper

class CustomerMapper(Mapper):

    def map(self):
        for row in self:
            self.emit(row[2], row[1])

    def read(self):
        reader = csv.reader(self.infile)
        for row in reader:
            yield row

if __name__ == '__main__':
    mapper = CustomerMapper()
    mapper.map()
