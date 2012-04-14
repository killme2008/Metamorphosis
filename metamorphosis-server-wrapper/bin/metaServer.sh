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
   	echo "$JAVA $BROKER_ARGS -Dcom.sun.management.jmxremote  -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
   	  -Dcom.sun.management.jmxremote.port=$JMX_PORT com.taobao.metamorphosis.ServerStartup -f $BASE_DIR/conf/server.ini"
    
    nohup $JAVA $BROKER_ARGS -Dcom.sun.management.jmxremote  -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
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

function status(){
    if [ -n "`ps ax | grep -i 'com.taobao.metamorphosis.ServerStartup' |grep java | grep -v grep | awk '{print $1}'`" ]; then
       echo "Meta broker is running."
    else
       echo "Meta broker was stopped."   
    fi
}

function reload_config() {
	echo "Reloading broker config..."
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.ReloadConfig -host 127.0.0.1 -port $JMX_PORT $@
}

function open_partitions() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.OpenPartitionsTool -host 127.0.0.1 -port $JMX_PORT $@
}

function close_partitions() {
	echo $@
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.ClosePartitionsTool -host 127.0.0.1 -port $JMX_PORT $@
}

function delete_partitions() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.DeletePartitionFiles $@
}

function move_partitions() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.MovePartitionFiles $@
}

function do_query() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.query.Bootstrap -s $BASE_DIR/conf/server.ini $@
}

function do_stats() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.BrokerStatsTool -config $BASE_DIR/conf/server.ini $@
}
 
function help() {
    echo "Usage: metaServer.sh {start|status|stop|restart|reload|stats|open-partitions|close-partitions|move-partitions|delete-partitions|query}" >&2
    echo "       start:             start the broker server"
    echo "       stop:              stop the broker server"
    echo "       status:            get broker current status,running or stopped."
    echo "       restart:           restart the broker server"
    echo "       reload:            reload broker config file server.ini when adding new topics etc."
    echo "       stats:             get current broker's statistics"
    echo "       open-partitions:   open partitions which were closed by close-partitions for topic"
    echo "       close-partitions:  close partitions for topic,closed partitions can not put messages until reopen them by open-partitions"
    echo "       query:             start command shell for querying offsets from zookeeper"     
}

command=$1
shift 1
case $command in
    start)
        start_server $@;
        ;;    
    stop)
        stop_server $@;
        ;;
    reload)
        reload_config $@;
        ;;    
    restart)
        $0 stop
        $0 start
        ;;
    status)
    	status $@;
        ;;    
    query)
        do_query $@;
        ;;
    open-partitions)
        open_partitions $@;
        ;;
    close-partitions)
        close_partitions $@;
        ;;
    delete-partitions)
        delete_partitions $@;
        ;;
    move-partitions)
        move_partitions $@;
        ;;
    stats)
        do_stats $@;
        ;;
    help)
        help;
        ;;
    *)
        help;
        exit 1;
        ;;
esac