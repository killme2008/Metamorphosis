#!/bin/bash

if [ $# -lt 2 ];
then
	echo "USAGE: $0 -f server.properties -Ftimetunnel timetunnel.properties Fnotify notify.properties"
	exit 1
fi
LOGFILE=$(dirname $0)/../logs/metaServer.log

nohup sh $(dirname $0)/meta-run-class.sh com.taobao.metamorphosis.ServerStartup $@ 2>&1 >>$LOGFILE &
tail $LOGFILE -f
