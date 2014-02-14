#!/bin/bash
#project directory

if [ -z "$BASE_DIR" ] ; then
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG="`dirname "$PRG"`/$link"
    fi
  done
  BASE_DIR=`dirname "$PRG"`/..

  # make it fully qualified
  BASE_DIR=`cd "$BASE_DIR" && pwd`
  #echo "Meta broker is at $BASE_DIR"
fi

source $BASE_DIR/bin/env.sh

AS_USER=`whoami`
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/metaServer.log"
PID_DIR="$BASE_DIR/logs"
PID_FILE="$PID_DIR/.run.pid"

if $enableHttp ; then
	config_files="-FjettyBroker=$BASE_DIR/conf/jettyBroker.properties"
fi

function running(){
	if [ -f "$PID_FILE" ]; then
		pid=$(cat "$PID_FILE")
		if [ -z "$pid" ]; then
			return 1;
		process=`ps aux | grep " $pid " | grep -v grep`;
		if [ -z "$process" ]; then
	    		return 1;
		else
			return 0;
		fi
	else
		return 1
	fi	
}

function start_server() {
	if running; then
		echo "Broker is running."
		exit 1
	fi

    mkdir -p $PID_DIR
    touch $LOG_FILE
    mkdir -p $LOG_DIR
    chown -R $AS_USER $PID_DIR
    chown -R $AS_USER $LOG_DIR
    
    config_files="$config_files -f $BASE_DIR/conf/server.ini"
    
    case $1 in
    	slave)
    		echo "Starting meta broker as an asynchronous replication slave...";
    		config_files="$config_files -Fmetaslave=$BASE_DIR/conf/async_slave.properties";
    		;;
	    samsa)
	        echo "Starting meta broker as a synchronous replication master...";
	        config_files="$config_files -Fsamsa=$BASE_DIR/conf/samsa_master.properties";
	        ;;
	    gregor)
	        echo "Starting meta broker as a synchronous replication slave...";
	        config_files="$config_files -Fgregor=$BASE_DIR/conf/gregor_slave.properties";
	        ;;
	    local)
	        echo "Starting meta broker in local mode...";
	        config_files="$config_files -l";
	        ;;    
	    *)
	    	echo "Starting meta broker..."
	    	;;	            		
    esac
    
   	echo "$JAVA $BROKER_ARGS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
   	  -Dcom.sun.management.jmxremote.port=$JMX_PORT com.taobao.metamorphosis.ServerStartup $config_files"
    sleep 1
    nohup $JAVA $BROKER_ARGS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
      -Dcom.sun.management.jmxremote.port=$JMX_PORT com.taobao.metamorphosis.ServerStartup $config_files 2>&1 >>$LOG_FILE &	
    echo $! > $PID_FILE
    chmod 755 $PID_FILE
	sleep 1;
	tail -F $LOG_FILE
}

function stop_server() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
	count=0
	pid=$(cat $PID_FILE)
	
	while running;
	do
	  let count=$count+1
	  echo "Stopping meta broker $count times"
	  if [ $count -gt 5 ]; then
	  	  echo "kill -9 $pid"
	      kill -9 $pid
	  else
	      $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.StopBrokerTool -host 127.0.0.1 -port $JMX_PORT $@
	      kill $pid
	  fi
	  sleep 3;
	done	
	rm $PID_FILE
	echo "Stop meta broker successfully." 	
}

function status(){
    if running; then
       echo "Meta broker is running."
    else
       echo "Meta broker was stopped."  
    fi
}

function reload_config() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
	echo "Reloading broker config..."
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.ReloadConfig -host 127.0.0.1 -port $JMX_PORT $@
}

function slave_status() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
	echo "Getting asynchronous slave status..."
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.SlaveStatus -host 127.0.0.1 -port $JMX_PORT $@
}

function open_partitions() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.OpenPartitionsTool -host 127.0.0.1 -port $JMX_PORT $@
}

function close_partitions() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.ClosePartitionsTool -host 127.0.0.1 -port $JMX_PORT $@
}

function delete_partitions() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.DeletePartitionFiles $@
}

function move_partitions() {
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.MovePartitionFiles $@
}

function do_query() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.query.Bootstrap -s server.ini $@
}

function do_stats() {
	if ! running; then
		echo "Broker is not running."
		exit 1
	fi
    $JAVA $TOOLS_ARGS com.taobao.metamorphosis.tools.shell.BrokerStatsTool -config server.ini $@
}
 
function help() {
    echo "Usage: metaServer.sh {start|status|stop|restart|reload|stats|open-partitions|close-partitions|move-partitions|delete-partitions|query}" >&2
    echo "       start [type]:             start the broker server"
    echo "             local               Start the broker in local mode,it will start a embed zookeeper,just for development or test."
    echo "             slave               start the broker as an asynchronous replication slave."
    echo "             gregor              start the broker as an synchronous replication slave."
    echo "             samsa               start the broker as an synchronous replication master."
    echo "       stop:              stop the broker server"
    echo "       status:            get broker current status,running or stopped."
    echo "       slave-status:      get broker(it must be an asynchronous slave) current status,replication status etc."
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
    slave-status)
        slave_status $@;
        ;;     
    restart)
        $0 stop $@
        $0 start $@
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
