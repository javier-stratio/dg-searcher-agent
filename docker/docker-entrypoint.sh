#!/usr/bin/env bash

source /docker/b-log.sh
source /docker/kms_utils.sh
source /docker/lib/logging.sh
INFO "Stratio scripting utils loaded..."

[[ "$DEBUG" == true ]] && INFO "Configuring script with DEBUG options..." && env && set -x

export PROFILE="${PROFILE:-default}" && infoLogging "Activating '$PROFILE' profile..."

export MODULE="dg-indexer"
export USER="governance"
export GROUP="stratio"
export ENVIRONMENT=/docker/lib/$PROFILE/environment.sh
export SECRETS=/docker/lib/$PROFILE/secrets.sh
export POSTGRES=/docker/lib/$PROFILE/postgres.sh
export TRUSTSTORE=/docker/lib/$PROFILE/truststore.sh
export CONFIG=/docker/lib/process.sh

[[ -f $ENVIRONMENT ]] && source $ENVIRONMENT  || {  errorLogging "$ENVIRONMENT No such file........" 1000; exit 10; }
[[ -f $SECRETS ]] && source $SECRETS  || {  errorLogging "$SECRETS No such file........" 1001; exit 10; }
[[ -f $POSTGRES  ]] && source $POSTGRES ||  { errorLogging "$POSTGRES No such file........" 1003; exit 10; }
[[ -f $TRUSTSTORE  ]] && source $TRUSTSTORE ||  { errorLogging "$TRUSTSTORE No such file........" 1006; exit 10; }
[[ -f $CONFIG  ]] && source $CONFIG ||  { errorLogging "$CONFIG No such file........" 1005; exit 10; }

infoLogging "su-exec $USER:1000 $JAVA_HOME/bin/java $JAVA_OPTS -jar /opt/sds/$MODULE/$JAR"
exec su-exec $USER:1000 $JAVA_HOME/bin/java $JAVA_OPTS -jar /opt/sds/$MODULE/$JAR
