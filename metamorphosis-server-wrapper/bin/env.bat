@echo off

if "%JAVA_HOME%" == "" (
   echo Plase set JAVA_HOME at first
   pause
   exit)

set JAVA="%JAVA_HOME%\bin\java"

REM meta home directory
set meta_home="%~dp0.."

REM jmx port
set JMX_PORT=9123
set CLASSPATH="%CLASSPATH%;%meta_home%\lib\*;%meta_home%\conf\*"

REM broker args
set JVM_ARGS= -Xmx512m -Xms512m
set BROKER_ARGS= %JVM_ARGS% -Dmeta.home=%meta_home% -cp %CLASSPATH%  -Dlog4j.configuration=file:%meta_home%\bin\log4j.properties




