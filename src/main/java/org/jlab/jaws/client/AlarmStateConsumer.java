package org.jlab.jaws.client;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.jlab.jaws.eventsource.EventSourceConfig;
import org.jlab.jaws.eventsource.EventSourceListener;
import org.jlab.jaws.eventsource.EventSourceRecord;
import org.jlab.jaws.eventsource.EventSourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class AlarmStateConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlarmStateConsumer.class);

    public static void main(String[] args) throws InterruptedException {
        final String servers = args[0];

        final Properties props = new Properties();

        props.put(EventSourceConfig.EVENT_SOURCE_TOPIC, "alarm-state");
        props.put(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, servers);
        props.put(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");

        // Deserializer specific configs
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://registry:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,"true");

        final EventSourceTable<String, String> consumer = new EventSourceTable<>(props);

        consumer.addListener(new EventSourceListener<>() {
            @Override
            public void initialState(Set<EventSourceRecord<String, String>> records) {
                for (EventSourceRecord<String, String> record : records) {
                    String key = record.getKey();
                    String value = record.getValue();
                    System.out.println(key + "=" + value);
                }
                consumer.close();
            }
        });

        consumer.start();
        consumer.join(); // block until first update, which contains current state of topic
    }
}
