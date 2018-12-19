#!/usr/bin/env bash

errorLogging(){
    [[ "$DEBUG" == true ]] && set +x
    ERROR $1
    ERROR "https://stratio.atlassian.net/wiki/spaces/GOVERNANCE0X1X0/overview - $2"
    [[ "$DEBUG" == true ]] && set -x
}

infoLogging(){
    [[ "$DEBUG" == true ]] && set +x
    INFO $1
    [[ "$DEBUG" == true ]] && set -x
}