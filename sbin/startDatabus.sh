#!/bin/bash
CP="lib/*::target/databus.jar"

OPTS="-server -Xms512m -Xmx4096m -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintPromotionFailure -Xloggc:logs/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"
#OPTS="-server -Xms512m -Xmx4096m"

MAIN_CLASS="databus.boot.DatabusMain"
if [ $# -ge 1 ]
then CONFIG_FILE=$1
else CONFIG_FILE="conf/databus.xml"
fi

if [ $# -ge 2 ]
then PID_FILE=$2
else PID_FILE="data/databus.pid"
fi

java $OPTS -cp $CP $MAIN_CLASS $CONFIG_FILE $PID_FILE
