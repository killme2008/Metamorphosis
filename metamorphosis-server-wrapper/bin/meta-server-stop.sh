#!/bin/sh
ps ax | grep -i 'com.taobao.metamorphosis.ServerStartup' |grep java | grep -v grep | awk '{print $1}' | xargs kill 
