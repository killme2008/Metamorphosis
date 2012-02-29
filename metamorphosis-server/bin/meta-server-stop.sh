#!/bin/sh
ps ax | grep -i 'com.taobao.metamorphosis.server.MetamorphosisStartup' |grep java | grep -v grep | awk '{print $1}' | xargs kill 
