#!/bin/bash
CP="lib/commons-beanutils-1.9.3.jar:lib/commons-collections-3.2.2.jar:lib/commons-collections4-4.1.jar:lib/commons-compress-1.11.jar:lib/commons-dbcp2-2.2.0.jar:lib/commons-io-2.5.jar:lib/commons-lang3-3.7.jar:lib/commons-logging-1.2.jar:lib/commons-pool2-2.5.0.jar:lib/gson-2.8.2.jar:lib/guava-24.1-jre.jar:lib/jedis-2.9.0.jar:lib/jsr305-1.3.9.jar:lib/jts-1.13.jar:lib/kafka-clients-1.1.0.jar:lib/log4j-api-2.11.0.jar:lib/log4j-core-2.11.0.jar:lib/log4j-jcl-2.11.0.jar:lib/log4j-slf4j-impl-2.11.0.jar:lib/lz4-java-1.4.jar:lib/maven-aether-provider-3.0.jar:lib/mysql-binlog-connector-java-0.16.0.jar:lib/mysql-connector-java-5.1.46.jar:lib/open-replicator-1.6.0.jar:lib/slf4j-api-1.7.25.jar:lib/snappy-0.4.jar:lib/snappy-java-1.1.7.1.jar:lib/spring-aop-5.0.5.RELEASE.jar:lib/spring-beans-5.0.5.RELEASE.jar:lib/spring-context-5.0.5.RELEASE.jar:lib/spring-core-5.0.5.RELEASE.jar:lib/spring-expression-5.0.5.RELEASE.jar:lib/spring-jcl-5.0.5.RELEASE.jar:target/databus.jar"

OPTS="-server -Xms512m -Xmx4096m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:logs/gc.log"
#OPTS="-server -Xms512m -Xmx4096m"

MAIN_CLASS="databus.application.DatabusMain"
if [ $# -ge 1 ]
then CONFIG_FILE=$1
else CONFIG_FILE="conf/databus.xml"
fi

if [ $# -ge 2 ]
then PID_FILE=$2
else PID_FILE="data/databus.pid"
fi

java $OPTS -cp $CP $MAIN_CLASS $CONFIG_FILE $PID_FILE
