#!/bin/bash

sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.ReloadConfig $@ 

sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.SlaveResubscribe $@ 
