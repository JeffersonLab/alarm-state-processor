package org.jlab.alarms.eventsource;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class EventSourceConfig extends AbstractConfig {
    public static final String EVENT_SOURCE_TOPIC = "event.source.topic";
    public static final String EVENT_SOURCE_POLL_MILLIS = "event.source.poll.millis";
    public static final String EVENT_SOURCE_MAX_POLL_BEFORE_FLUSH = "event.source.max.poll.before.flush";

    // Use the same identifiers as ConsumerConfig as we'll pass 'em right on through
    public static final String EVENT_SOURCE_GROUP = "group.id";
    public static final String EVENT_SOURCE_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String EVENT_SOURCE_KEY_DESERIALIZER = "key.deserializer";
    public static final String EVENT_SOURCE_VALUE_DESERIALIZER = "value.deserializer";

    public EventSourceConfig(Map originals) {
        super(configDef(), originals, false);
    }

    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define(EVENT_SOURCE_TOPIC,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka event source topic to monitor")
                .define(EVENT_SOURCE_POLL_MILLIS,
                        ConfigDef.Type.LONG,
                        1000l,
                        ConfigDef.Importance.HIGH,
                        "Milliseconds between polls for topic changes - notification delay is EVENT_SOURCE_MAX_POLL_BEFORE_FLUSH times this value with a constant stream of changes, or twice this value when changes are intermittent since the consumer thread waits for 'no changes' poll response before notifying listeners")
                .define(EVENT_SOURCE_MAX_POLL_BEFORE_FLUSH,
                        ConfigDef.Type.LONG,
                        5l,
                        ConfigDef.Importance.HIGH,
                        "Max number of polls before forcing a flush - ensures changes are flushed in case of a constant stream of changes occurring more frequently than poll duration.  Set to 1 to force flush after every poll with changes")
                .define(EVENT_SOURCE_GROUP,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka consumer group to use when monitoring the EVENT_SOURCE_TOPIC")
                .define(EVENT_SOURCE_BOOTSTRAP_SERVERS,
                        ConfigDef.Type.STRING,
                        "localhost:9092",
                        ConfigDef.Importance.HIGH,
                        "Comma-separated list of host and port pairs that are the addresses of the Kafka brokers used to query the EVENT_SOURCE_TOPIC")
                .define(EVENT_SOURCE_KEY_DESERIALIZER,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the key")
                .define(EVENT_SOURCE_VALUE_DESERIALIZER,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the value");
    }
}
