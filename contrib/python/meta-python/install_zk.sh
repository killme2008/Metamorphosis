#!/bin/sh
wget http://labs.renren.com/apache-mirror//zookeeper/zookeeper-3.4.3/zookeeper-3.4.3.tar.gz
tar xzvf zookeeper-3.4.3.tar.gz
cd zookeeper-3.4.3/src/c
./configure --includedir=/usr/include
make
sudo make install
