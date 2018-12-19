#!/usr/bin/env bash

export LOGGING_CONFIG="file:/etc/sds/${MODULE}/logback.xml"

export CGROUP_MEMORY=$(($(sed -n '/^hierarchical_memory_limit/p' /sys/fs/cgroup/memory/memory.stat | awk '{print $2}')/1000/1000-100))
export JAVA_OPTS="$JAVA_OPTS    -server
                                -Xms256m -Xmx$(($CGROUP_MEMORY*90/100))m
                                -XX:MaxMetaspaceSize=100m
                                -Xmn$(($CGROUP_MEMORY*30/100))m
                                -XX:SurvivorRatio=4
                                -XX:+CMSClassUnloadingEnabled
                                -XX:+UseConcMarkSweepGC
                                -XX:+CMSParallelRemarkEnabled
                                -XX:+UseCMSInitiatingOccupancyOnly
                                -XX:CMSInitiatingOccupancyFraction=70
                                -XX:+ScavengeBeforeFullGC
                                -XX:+CMSScavengeBeforeRemark
                                -XX:+PrintGCDateStamps
                                -verbose:gc
                                -XX:+PrintGCDetails
                                -Xloggc:/tmp/loggc.log
                                -XX:+UseGCLogFileRotation
                                -XX:NumberOfGCLogFiles=10
                                -XX:GCLogFileSize=100M
                                -Dnetworkaddress.cache.ttl=10
                                -Dnetworkaddress.cache.negative.ttl=4
                                -XX:+HeapDumpOnOutOfMemoryError
                                -XX:HeapDumpPath=/mnt/mesos/sandbox/dump.hprof"

export JAVA_OPTS="$JAVA_OPTS    -Dliquibase.databaseChangeLogTableName=$MODULE-ChangeLog
                                -Dliquibase.databaseChangeLogLockTableName=$MODULE-ChangeLogLock
                                -Dlogback.configurationFile=$LOGGING_CONFIG"


