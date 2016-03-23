#!/usr/bin/env python

import csv
from framework import Mapper

class IdentityMapper(Mapper):

    def map(self):
        for row in self:
            key, value = row.split(self.sep)
            self.emit(key, value)

if __name__ == '__main__':
    mapper = IdentityMapper()
    mapper.map()
