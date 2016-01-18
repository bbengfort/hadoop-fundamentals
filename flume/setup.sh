#!/bin/bash
sudo -u hdfs hadoop fs -mkdir -p /user/hadoop/impressions/
sudo -u hdfs hadoop fs -chmod 1777 /user/hadoop/impressions/

sudo mkdir /tmp/impressions
sudo chmod 777 /tmp/impressions

sudo mkdir /tmp/flume
sudo chmod 1777 /tmp/flume
