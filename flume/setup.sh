#!/bin/bash
sudo -u hadoop hadoop fs -mkdir -p /user/hadoop/impressions/
sudo -u hadoop hadoop fs -chmod 1777 /user/hadoop/impressions/

sudo mkdir /tmp/impressions
sudo chmod 777 /tmp/impressions

sudo mkdir /tmp/flume
sudo chmod 1777 /tmp/flume
