#!/bin/sh
if [  -d  zookeeper-3.4.3 ];then
	cd zookeeper-3.4.3/bin/
	./zkServer.sh stop
fi

