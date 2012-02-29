#!/bin/bash

# 分区编号(目录)向前或向后偏移,一般用于分区迁移时
# usage:
#      MovePartitionFiles -dataDir /home/admin/metadata -topic xxtopic -start 5 -end 10 -offset -5
 
sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.MovePartitionFiles $@ 
