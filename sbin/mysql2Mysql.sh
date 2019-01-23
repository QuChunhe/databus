#!/usr/bin/env bash
CP="lib/commons-dbcp2-2.2.0.jar:lib/commons-io-2.5.jar:lib/commons-lang3-3.7.jar:lib/commons-logging-1.2.jar:lib/commons-pool2-2.5.0.jar:lib/log4j-api-2.11.1.jar:lib/log4j-core-2.11.1.jar:lib/log4j-jcl-2.11.1.jar:lib/log4j-slf4j-impl-2.11.1.jar:lib/mysql-connector-java-5.1.46.jar:lib/slf4j-api-1.7.25.jar:lib/spring-aop-5.1.4.RELEASE.jar:lib/spring-beans-5.1.4.RELEASE.jar:lib/spring-context-5.1.4.RELEASE.jar:lib/spring-core-5.1.4.RELEASE.jar:lib/spring-expression-5.1.4.RELEASE.jar:lib/spring-jcl-5.1.4.RELEASE.jar:target/databus.jar"

OPTS="-server -Xms512m -Xmx4096m -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintPromotionFailure -Xloggc:logs/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M"
#OPTS="-server -Xms512m -Xmx4096m"

MAIN_CLASS="databus.boot.Mysql2MysqlMain"

java $OPTS -cp $CP $MAIN_CLASS $@