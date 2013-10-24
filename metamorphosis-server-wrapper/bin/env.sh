#!/bin/bash

#Config your java home
#JAVA_HOME=/opt/jdk/

if [ -z "$JAVA_HOME" ]; then
  export JAVA=`which java`
else
  export JAVA="$JAVA_HOME/bin/java"
fi

#JMX port
meta_home=$BASE_DIR

#Broker JMX port
export JMX_PORT=9123
export CLASSPATH=$CLASSPATH:$BASE_DIR/conf:$(ls $BASE_DIR/lib/*.jar | tr '\n' :)

#Broker jvm args
BROKER_JVM_ARGS="-Xmx512m -Xms512m -server -Dmeta.home=$meta_home -cp $CLASSPATH "
#Tools jvm args,you don't have to modify this at all.
TOOLS_JVM_ARGS="-Xmx128m -Xms128m -Dmeta.home=$meta_home -cp $CLASSPATH "

#whether to enable http endpoints
export enableHttp=false

if [ -z "$BROKER_ARGS" ]; then
  export BROKER_ARGS="$BROKER_JVM_ARGS -Dlog4j.configuration=file:$BASE_DIR/bin/log4j.properties"
fi

if [ -z "$TOOLS_ARGS" ]; then
  export TOOLS_ARGS="$TOOLS_JVM_ARGS -Dlog4j.configuration=file:$BASE_DIR/bin/tools_log4j.properties"
fi





