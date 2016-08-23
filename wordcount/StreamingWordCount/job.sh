#!/bin/bash

#start hadoop
#start-all.sh

HADOOP_PREFIX=/srv/hadoop

$HADOOP_PREFIX/bin/hadoop jar $HADOOP_PREFIX/contrib/streaming/hadoop-*streaming*.jar \
    -file mapper.py -file reducer.py \
    -mapper mapper.py -reducer reducer.py -combiner reducer.py \
    -input $1 -output $2


#input file:
#/user/hadoop/gutenberg

#output file:
#/user/hadoop/gutenberg-out
