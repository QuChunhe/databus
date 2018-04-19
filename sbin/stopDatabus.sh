#!/bin/bash
if [ $# -ge 1 ]
then PID_FILE=$1
else echo "Must add pid file as arg!"
fi
kill -15 `cat $PID_FILE`
