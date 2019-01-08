#!/bin/bash
CP="lib/aether-api-1.7.jar:lib/aether-impl-1.7.jar:lib/aether-spi-1.7.jar:lib/aether-util-1.7.jar:lib/animal-sniffer-annotations-1.14.jar:lib/asm-5.0.3.jar:lib/asm-analysis-5.0.3.jar:lib/asm-commons-5.0.3.jar:lib/asm-tree-5.0.3.jar:lib/asm-util-5.0.3.jar:lib/cassandra-driver-core-3.6.0.jar:lib/cassandra-driver-extras-3.6.0.jar:lib/checker-compat-qual-2.0.0.jar:lib/commons-beanutils-1.9.3.jar:lib/commons-collections-3.2.2.jar:lib/commons-collections4-4.1.jar:lib/commons-compress-1.11.jar:lib/commons-dbcp2-2.5.0.jar:lib/commons-io-2.5.jar:lib/commons-lang3-3.7.jar:lib/commons-logging-1.2.jar:lib/commons-pool2-2.6.0.jar:lib/error_prone_annotations-2.1.3.jar:lib/gson-2.8.5.jar:lib/guava-27.0-jre.jar:lib/j2objc-annotations-1.1.jar:lib/jedis-3.0.1.jar:lib/jffi-1.2.16.jar:lib/jffi-1.2.16-native.jar:lib/jnr-constants-0.9.9.jar:lib/jnr-ffi-2.1.7.jar:lib/jnr-posix-3.0.44.jar:lib/jnr-x86asm-1.0.2.jar:lib/jsr305-1.3.9.jar:lib/jts-1.13.jar:lib/kafka-clients-1.1.0.jar:lib/log4j-api-2.11.1.jar:lib/log4j-core-2.11.1.jar:lib/log4j-jcl-2.11.1.jar:lib/log4j-slf4j-impl-2.11.1.jar:lib/lz4-java-1.4.jar:lib/metrics-core-3.2.2.jar:lib/mysql-binlog-connector-java-0.16.1.jar:lib/mysql-connector-java-5.1.46.jar:lib/netty-buffer-4.1.25.Final.jar:lib/netty-codec-4.1.25.Final.jar:lib/netty-common-4.1.25.Final.jar:lib/netty-handler-4.1.25.Final.jar:lib/netty-resolver-4.1.25.Final.jar:lib/netty-transport-4.1.25.Final.jar:lib/netty-transport-native-epoll-4.1.25.Final.jar:lib/netty-transport-native-unix-common-4.1.25.Final.jar:lib/open-replicator-1.6.0.jar:lib/plexus-archiver-3.4.jar:lib/plexus-build-api-0.0.7.jar:lib/plexus-cipher-1.4.jar:lib/plexus-classworlds-2.2.3.jar:lib/plexus-component-annotations-1.5.5.jar:lib/plexus-interpolation-1.22.jar:lib/plexus-io-2.7.1.jar:lib/plexus-sec-dispatcher-1.3.jar:lib/plexus-utils-3.0.24.jar:lib/sisu-guice-2.1.7-noaop.jar:lib/sisu-inject-bean-1.4.2.jar:lib/sisu-inject-plexus-1.4.2.jar:lib/slf4j-api-1.7.25.jar:lib/snappy-0.4.jar:lib/snappy-java-1.1.7.1.jar:lib/spring-aop-5.1.1.RELEASE.jar:lib/spring-beans-5.1.1.RELEASE.jar:lib/spring-context-5.1.1.RELEASE.jar:lib/spring-core-5.1.1.RELEASE.jar:lib/spring-expression-5.1.1.RELEASE.jar:lib/spring-jcl-5.1.1.RELEASE.jar:lib/xmlpull-1.1.3.1.jar:lib/xpp3_min-1.1.4c.jar:lib/xstream-1.4.9.jar:lib/xz-1.5.jar:target/databus.jar"

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
