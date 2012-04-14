#!/bin/bash

#project directory
BASE_DIR=$(dirname $0)

if [ ${BASE_DIR:0:1} == \. ]; then
   BASE_DIR=${BASE_DIR/\./$(pwd)}
fi

export BASE_DIR=$BASE_DIR/..

source $BASE_DIR/bin/env.sh

AS_USER=`whoami`
LOG_DIR=$BASE_DIR/logs
LOG_FILE=$LOG_DIR/metaServer.log
PID_DIR=$BASE_DIR/logs

function start_server() {

    mkdir -p $PID_DIR
    touch $LOG_FILE
    mkdir -p $LOG_DIR
    chown -R $AS_USER $PID_DIR
    chown -R $AS_USER $LOG_DIR

   	echo "Starting meta broker..."
   	echo "$JAVA $ARGS -Dcom.sun.management.jmxremote  -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
   	  -Dcom.sun.management.jmxremote.port=$JMX_PORT com.taobao.metamorphosis.ServerStartup -f $BASE_DIR/conf/server.ini"
    
    nohup $JAVA $ARGS -Dcom.sun.management.jmxremote  -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
      -Dcom.sun.management.jmxremote.port=$JMX_PORT com.taobao.metamorphosis.ServerStartup -f $BASE_DIR/conf/server.ini $@ 2>&1 >>$LOG_FILE &	
	sleep 1;
	tail -f $LOG_FILE
}

function stop_server() {
	count=0
	while [ -n "`ps ax | grep -i 'com.taobao.metamorphosis.ServerStartup' |grep java | grep -v grep | awk '{print $1}'`" ]
	do
	  let count=$count+1
	  if [ $count -gt 5 ]; then
	      ps ax | grep -i 'com.taobao.metamorphosis.ServerStartup' |grep java | grep -v grep | awk '{print $1}' | xargs kill -9
	  else
	      ps ax | grep -i 'com.taobao.metamorphosis.ServerStartup' |grep java | grep -v grep | awk '{print $1}' | xargs kill 
	  fi
	  sleep 2;
	done	
	echo "Stop meta broker successfully." 
}

function reload_config() {
	echo "Reloading broker config..."
    $JAVA $ARGS com.taobao.metamorphosis.tools.shell.ReloadConfig -host 127.0.0.1 -port $JMX_PORT
}

case "$1" in
    start)
        start_server;
        ;;    
    stop)
        stop_server;
        ;;
    reload)
        reload_config;
        ;;    
    restart)
        $0 stop
        $0 start
        ;;
    
    *)
        echo "Usage: metaServer.sh {start|status|stop|restart|reload|stats}" >&2
        exit 1
        ;;
esac