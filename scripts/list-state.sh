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
EXTRA=`ls $APP_HOME/lib/kafka-avro-serializer-*`
EXTRA2=`ls $APP_HOME/lib/common-utils-*`
EXTRA3=`ls $APP_HOME/lib/kafka-schema-registry-client-*`
EXTRA4=`ls $APP_HOME/lib/kafka-schema-serializer-*`
EXTRA5=`ls $APP_HOME/lib/avro-*`
EXTRA6=`ls $APP_HOME/lib/jakarta.ws.rs-api-*`
EXTRA7=`ls $APP_HOME/lib/jersey-common-*`
STREAMS1=`ls $APP_HOME/lib/kafka-streams-2.*.jar`
STREAMS2=`ls $APP_HOME/lib/kafka-streams-avro-serde-*`

RUN_CP="$APP_JAR:$JAWS_JAR:$CLIENTS_JAR:$SLF4J_API:$SLF4J_IMP:$LOG4J_IMP:$LOG4J_CONF:$JACK_CORE:$JACK_BIND:$JACK_ANN:$EXTRA:$EXTRA2:$EXTRA3:$EXTRA4:$EXTRA5:$EXTRA6:$EXTRA7:$STREAMS1:$STREAMS2"

java -Dlog.dir=$APP_HOME/logs -Dlog4j.configuration="file://$APP_HOME/config/log4j-client.properties" -cp $RUN_CP org.jlab.jaws.client.AlarmStateConsumer $BOOTSTRAP_SERVERS alarm-state