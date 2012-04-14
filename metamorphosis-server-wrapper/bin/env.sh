#!/bin/bash

#project directory
BASE_DIR=$(dirname $0)

if [ ${BASE_DIR:0:1} == \. ]; then
   BASE_DIR=${BASE_DIR/\./$(pwd)}
fi

export BASE_DIR=$BASE_DIR/..

if [ -z "$JAVA_HOME" ]; then
  export JAVA=`which java`
else
  export JAVA="$JAVA_HOME/bin/java"
fi

#JMX port
meta_home=$(cd $BASE_DIR;pwd)
export JMX_PORT=9123
export CLASSPATH=$CLASSPATH:$BASE_DIR/conf:$(ls $BASE_DIR/lib/*.jar | tr '\n' :)
export JVM_ARGS="-Xmx512m -Xms512m -server -Dmeta.home=$meta_home -cp $CLASSPATH "

if [ -z "$ARGS" ]; then
  export ARGS="$JVM_ARGS -Dlog4j.configuration=$BASE_DIR/bin/log4j.properties"
fi





