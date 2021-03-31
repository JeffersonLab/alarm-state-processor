#!/bin/bash

[ -z "$BOOTSTRAP_SERVERS" ] && echo "BOOTSTRAP_SERVERS environment required" && exit 1;

CWD=$(readlink -f "$(dirname "$0")")

APP_HOME=$CWD/..

APP_JAR=`ls $APP_HOME/lib/alarm-state-processor*`
JAWS_JAR=`ls $APP_HOME/lib/jaws-libj*`
CLIENTS_JAR=`ls $APP_HOME/lib/kafka-clients-*`
JACK_CORE=`ls $APP_HOME/lib/jackson-core-*`
JACK_BIND=`ls $APP_HOME/lib/jackson-databind-*`
JACK_ANN=`ls $APP_HOME/lib/jackson-annotations-*`
SLF4J_API=`ls $APP_HOME/lib/slf4j-api-*`
SLF4J_IMP=`ls $APP_HOME/lib/slf4j-log4j*`
LOG4J_IMP=`ls $APP_HOME/lib/log4j-*`

RUN_CP="$APP_JAR:$JAWS_JAR:$CLIENTS_JAR:$SLF4J_API:$SLF4J_IMP:$LOG4J_IMP:$LOG4J_CONF:$JACK_CORE:$JACK_BIND:$JACK_ANN"

java -Dlog.dir=$APP_HOME/logs -Dlog4j.configuration="file://$APP_HOME/config/log4j-client.properties" -cp $RUN_CP org.jlab.jaws.client.AlarmStateConsumer $BOOTSTRAP_SERVERS alarm-state