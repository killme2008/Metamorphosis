#!/bin/bash

# 清除一个topic的某些分区数据
# usage:
#      DeletePartitionFiles -dataDir /home/admin/metadata -topic xxtopic -start 5 -end 10
 
sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.DeletePartitionFiles $@ 
