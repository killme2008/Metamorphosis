@echo off

REM set your java home
REM JAVA_HOME=C:/jdk
set JAVA=%JAVA_HOME%/bin/java

REM meta home directory
set meta_home=%~dp0..

REM jmx port
set JMX_PORT=9123
set CLASSPATH=%CLASSPATH%;%meta_home%\lib\*;%meta_home%\conf\*


REM broker jvm args
set BROKER_JVM_ARGS="-Xmx512m -Xms512m -server -Dmeta.home=%meta_home% -cp %CLASSPATH% "

REM tools jvm args,you don't have to modify this at all.
set TOOLS_JVM_ARGS="-Xmx128m -Xms128m -Dmeta.home=%meta_home% -cp %CLASSPATH%"

REM broker and tools args
set BROKER_ARGS="%BROKER_JVM_ARGS% -Dlog4j.configuration=file:%meta_home%/bin/log4j.properties"
set TOOLS_ARGS="%TOOLS_JVM_ARGS% -Dlog4j.configuration=file:%meta_home%/bin/tools_log4j.properties"


