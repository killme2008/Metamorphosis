#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

CLASSPATH=$CLASSPATH:$base_dir/conf


if [ -z "$META_TOOLS_OPTS" ]; then
  META_TOOLS_OPTS="-Djava.ext.dirs=$base_dir/lib -Dlog4j.configuration=$base_dir/bin/log4j.xml "
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $META_TOOLS_OPTS -classpath $CLASSPATH $@