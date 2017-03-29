#!/bin/bash
CP="lib/commons-beanutils-1.9.2.jar:lib/commons-collections4-4.1.jar:lib/commons-dbcp2-2.1.1.jar:lib/commons-lang3-3.5.jar:lib/commons-logging-1.2.jar:lib/commons-pool2-2.4.2.jar:lib/gson-2.8.0.jar:lib/guava-21.0.jar:lib/jedis-2.9.0.jar:lib/jts-1.13.jar:lib/kafka-clients-0.10.0.0.jar:lib/log4j-api-2.8.1.jar:lib/log4j-core-2.8.1.jar:lib/log4j-slf4j-impl-2.8.1.jar:lib/log4j-jcl-2.8.1.jar:lib/mysql-connector-java-5.1.40.jar:lib/open-replicator-1.6.0.jar:lib/slf4j-api-1.7.25.jar:lib/spring-context-4.3.5.RELEASE.jar:lib/spring-core-4.3.5.RELEASE.jar:lib/spring-beans-4.3.5.RELEASE.jar:lib/spring-beans-4.3.5.RELEASE.jar:lib/spring-core-4.3.5.RELEASE.jar:lib/spring-expression-4.3.5.RELEASE.jar:databus.jar"

OPTS="-server -Xms512m -Xmx4096m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:logs/gc.log"
#OPTS="-server -Xms512m -Xmx4096m"

MAIN_CLASS="databus.application.DatabusMain"
if [ $# -ge 1 ]
then PARAM=$1
else PARAM="conf/databus.xml"
fi
java $OPTS -cp $CP $MAIN_CLASS $PARAM
