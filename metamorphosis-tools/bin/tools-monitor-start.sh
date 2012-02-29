#!/bin/bash

#if [ $# -lt 2 ];
#then
#	echo "USAGE: $0 -f server.properties"
#	exit 1
#fi
LOGFILE=$(dirname $0)/../logs/meta-monitor.log

nohup sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.monitor.MonitorStartup $@ 2>&1 >>$LOGFILE &
tail $LOGFILE -f
