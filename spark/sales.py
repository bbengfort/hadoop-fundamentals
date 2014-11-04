## Count the number of sales per month in our order history.

## Imports
import csv

from operator import add
from datetime import datetime
from StringIO import StringIO
from collections import namedtuple
from pyspark import SparkConf, SparkContext

## Helper constants
DATE_FMT = "%Y-%m-%d %H:%M:%S" # 2013-09-16 12:23:33

## Named Tuple
Order = namedtuple('Order', ('id', 'upc', 'customer_id', 'date'))

def parse(row):
    """
    Parses a row and returns a named tuple.
    """
    row[0] = int(row[0]) # Parse ID to an integer
    row[2] = int(row[2]) # Parse Customer ID to an integer
    row[3] = datetime.strptime(row[3], DATE_FMT)
    return Order(*row)

def split(line):
    """
    Operator function for splitting a line on a delimiter.
    """
    reader = csv.reader(StringIO(line))
    return reader.next()

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("Sales Report")
    sc   = SparkContext(conf=conf)

    # Base RDD
    orders = sc.textFile("shopping/orders.csv")

    # Transform all rows into a parsed Orders object
    orders = orders.map(split).map(parse)

    # Transform orders to ensure just 2013 orders are tracked
    orders = orders.filter(lambda order: order.date.year == 2013)
    orders.cache() # Hint that we'll be using the cache

    # Transform to a mapping of (year, month) => count
    months = orders.map(lambda order: ((order.date.year, order.date.month), 1))

    # Transform to a mapping specifically concerning a particular product
    products = orders.filter(lambda order: order.upc == "098668274321")
    print products.toDebugString()

    # Sum Reducer Action 1
    months = months.reduceByKey(add)
    print months.toDebugString()
    print months.take(5)

    # # Count Action 2
    print products.count()
