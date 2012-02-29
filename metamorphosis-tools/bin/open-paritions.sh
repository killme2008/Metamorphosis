#!/bin/bash

# 清除一个topic的所有分区 的关闭标记,一般用在误操作关闭分区后的补救
# usage:
#      OpenPartitionsTool -topic xxtopic
#      OpenPartitionsTool -topic xxtopic -host 10.2.2.3
#      OpenPartitionsTool -topic xxtopic -port 9999
#      OpenPartitionsTool -topic xxtopic -host 10.2.2.3 -port 9999
 
sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.OpenPartitionsTool $@ 
