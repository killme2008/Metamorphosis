@echo off

setlocal
call "%~dp0env.bat"

set config_files= -f "%meta_home%\conf\server.ini"
echo on

%JAVA% %BROKER_ARGS% -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=%JMX_PORT% com.taobao.metamorphosis.ServerStartup %config_files% -l %*

endlocal


