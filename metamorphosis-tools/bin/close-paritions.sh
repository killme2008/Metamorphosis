#!/bin/bash

# 关闭一个topic的指定分区
# usage:
#      ClosePartitionsTool -topic xxtopic -start 2 -end 5
#      ClosePartitionsTool -topic xxtopic -start 2 -end 5 -host 10.2.2.3
#      ClosePartitionsTool -topic xxtopic -start 2 -end 5 -port 9999
#      ClosePartitionsTool -topic xxtopic -start 2 -end 5 -host 10.2.2.3 -port 9999

sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.ClosePartitionsTool $@ 
