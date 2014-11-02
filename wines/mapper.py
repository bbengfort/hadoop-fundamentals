#!/usr/bin/env python
"""
The mean (ybar) = 5.6360225140712945
the sstot = 1042.1651031894867
"""


import sys
import csv
import json

from framework import Mapper

fields = (
    "fixed_acidity", "volatile_acidity","citric_acid", "residual_sugar",
    "chlorides","free_sulfur_dioxide", "total_sulfur_dioxide","density",
    "pH","sulphates", "alcohol","quality"
)

def parse(row):
    """
    Parses a row to floats then returns a Wine tuple
    """
    return dict(zip(fields, [float(item) for item in row]))

class ThetaMapper(Mapper):

    def __init__(self, *args, **kwargs):
        super(ThetaMapper, self).__init__(*args, **kwargs)

        with open('theta.json', 'r') as thetas:
            self.thetas = json.load(thetas)

    def compute_cost(self, values):
        yhat = 0
        for key, val in self.thetas.items():
            yhat += values.get(key, 1) * val

        return (values['quality'] - yhat) ** 2

    def map(self):
        for wine in self:
            self.emit(1, self.compute_cost(wine))

    def __iter__(self):
        reader = csv.reader(self.read(), delimiter=";")
        for wine in map(parse, reader):
            yield wine

if __name__ == '__main__':
    mapper = ThetaMapper(sys.stdin)
    mapper.map()
