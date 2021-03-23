package org.jlab.alarms.client.extras;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.jlab.alarms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class StreamsAPITest {

    private static final Logger log = LoggerFactory.getLogger(StreamsAPITest.class);

    public static final String INPUT_TOPIC = "registered-alarms";

    public static final Serde<String> INPUT_KEY_SERDE = Serdes.String();
    public static final SpecificAvroSerde<RegisteredAlarm> INPUT_VALUE_SERDE = new SpecificAvroSerde<>();

    public static void main(String[] args) throws InterruptedException {
        final String servers = args[0];

        final Properties props = createProperties(servers,  "http://registry:8081");
        final Topology top = createTopology(props);
        final KafkaStreams streams = new KafkaStreams(top, props);

        // Runs forever, KafkaStreams alone cannot tell you what the high water mark is - you can use a separate
        // Consumer to get this info then use Processor API of streams to examine message index and close when == max
        // Or a time-based hack like if no records in a given time period then assume done.
        streams.start();
    }

    private static Properties createProperties(String bootstrapServers, String registryUrl) {
        final Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-api-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

        return props;
    }

    private static Topology createTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        INPUT_KEY_SERDE.configure(config, true);
        INPUT_VALUE_SERDE.configure(config, false);

        // KTable removes duplicate keys, I hope!
        final KTable<String, RegisteredAlarm> input = builder.table(INPUT_TOPIC, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

        //final KStream<String, RegisteredAlarm> input = builder.stream(INPUT_TOPIC, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

        input.toStream().foreach(new ForeachAction<String, RegisteredAlarm>() {
            @Override
            public void apply(String key, RegisteredAlarm value) {
                System.out.println(key + "=" + value);
            }
        });

        return builder.build();
    }
}

