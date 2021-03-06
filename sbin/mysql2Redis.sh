#!/usr/bin/env bash
CP="lib/*::target/databus.jar"

OPTS="-server -Xms512m -Xmx4096m -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintPromotionFailure -Xloggc:logs/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"
#OPTS="-server -Xms512m -Xmx4096m"

MAIN_CLASS="databus.boot.Mysql2RedisMain"

java $OPTS -Dlog4j.configurationFile=./conf/log4j2.xml -cp $CP $MAIN_CLASS $@