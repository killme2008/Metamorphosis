#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$META_OPTS" ]; then
  META_OPTS="-Xmx512m -server -Dcom.sun.management.jmxremote -Dlog4j.configuration=$base_dir/bin/log4j.properties "
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $META_OPTS -cp $CLASSPATH $@