#!/bin/bash

# tt
# usage:
#      tt-tool -start
#      tt-tool -stop
#      tt-tool -look
#      tt-tool -start -host 10.2.2.3 -port 9999

sh $(dirname $0)/tools-run-class.sh com.taobao.metamorphosis.tools.shell.TimetunnelPluginTool $@ 
