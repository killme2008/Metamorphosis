#!/bin/bash

if [ $# -lt 2 ];
then
        echo "USAGE: $0 -s server.properties -j jdbc.properties"
        exit 1
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi
base_dir=$(dirname $0)/..
META_TOOLS_OPTS="-Djava.ext.dirs=$base_dir/lib"
stty erase ^H
$JAVA $META_TOOLS_OPTS com.taobao.metamorphosis.tools.query.Bootstrap $@