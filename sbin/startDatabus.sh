#!/bin/bash
CP="./lib/commons-beanutils-1.9.2.jar:./lib/commons-collections4-4.1.jar:./lib/commons-configuration2-2.0.jar:./lib/commons-dbcp2-2.1.1.jar:./lib/commons-lang3-3.4.jar:./lib/commons-logging-1.2.jar:./lib/commons-pool2-2.4.2.jar:./lib/gson-2.6.1.jar:./lib/guava-19.0.jar:./lib/jedis2.7.3.jar:./lib/jts-1.13.jar:./lib/kafka-clients-0.10.0.0.jar:./lib/log4j-api-2.7.jar:./lib/log4j-core-2.7.jar:./lib/log4j-iostreams-2.7.jar:./lib/log4j-slf4j-impl-2.7.jar:./lib/log4j-jcl-2.7.jar:./lib/mysql-connector-java-5.1.37-bin.jar:./lib/netty-all-4.0.34.Final.jar:./lib/open-replicator.jar:./lib/slf4j-api-1.7.12.jar:./databus.jar"

OPTS="-server -Xms512m -Xmx4096m -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:logs/gc.log"
#OPTS="-server -Xms512m -Xmx4096m"

MAIN_CLASS="databus.application.DatabusStartup"
if [ $# -ge 1 ]
then PARAM=$1
else PARAM="conf/kafka_databus.xml"
fi
java $OPTS -cp $CP $MAIN_CLASS $PARAM
