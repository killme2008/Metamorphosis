#!/bin/sh
if [ ! -d  zookeeper-3.4.3 ];then
	wget http://labs.renren.com/apache-mirror//zookeeper/zookeeper-3.4.3/zookeeper-3.4.3.tar.gz
	tar xzvf zookeeper-3.4.3.tar.gz
fi

cd zookeeper-3.4.3/bin/
./zkServer.sh start