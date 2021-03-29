package org.jlab.jaws;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jlab.jaws.entity.*;
import org.jlab.jaws.eventsource.EventSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmStateProcessor {
    private static final Logger log = LoggerFactory.getLogger(AlarmStateProcessor.class);

    public static final String INPUT_TOPIC_REGISTERED = "registered-alarms";
    public static final String INPUT_TOPIC_ACTIVE = "active-alarms";
    public static final String INPUT_TOPIC_OVERRIDDEN = "overridden-alarms";

    public static final String OUTPUT_TOPIC = "alarm-state";

    public static final Serdes.StringSerde INPUT_KEY_REGISTERED_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde INPUT_KEY_ACTIVE_SERDE = new Serdes.StringSerde();
    public static final SpecificAvroSerde<OverriddenAlarmKey> INPUT_KEY_OVERRIDDEN_SERDE = new SpecificAvroSerde<>();

    public static final SpecificAvroSerde<RegisteredAlarm> INPUT_VALUE_REGISTERED_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarm> INPUT_VALUE_ACTIVE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<OverriddenAlarmValue> INPUT_VALUE_OVERRIDDEN_SERDE = new SpecificAvroSerde<>();


    public static final SpecificAvroSerde<DisabledAlarm> DISABLED_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde OUTPUT_KEY_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde OUTPUT_VALUE_SERDE = new Serdes.StringSerde();

    static AdminClient admin;

    static KafkaStreams streams;

    //static EventSourceTable<String, RegisteredAlarm> registeredTable;
    //static EventSourceTable<String, ActiveAlarm> activeTable;
    //static EventSourceTable<OverriddenAlarmKey, OverriddenAlarmValue> overriddenTable;

    final static CountDownLatch latch = new CountDownLatch(1);

    static Properties getRegisteredConfig() {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        Properties props = new Properties();
        props.put(EventSourceConfig.EVENT_SOURCE_TOPIC, "registered-alarms");
        props.put(EventSourceConfig.EVENT_SOURCE_GROUP, "AlarmStateProcessorRegistered");
        props.put(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, bootstrapServers);
        props.put(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, 5000);
        props.put(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        // Deserializer specific configs
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://registry:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,"true");

        return props;
    }

    static Properties getAdminConfig() {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        final Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "alarm-state-processor-admin");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    static Properties getStreamsConfig() {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        String registry = System.getenv("SCHEMA_REGISTRY");

        registry = (registry == null) ? "http://localhost:8081" : registry;

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "alarm-state-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registry);

        // https://stackoverflow.com/questions/57164133/kafka-stream-topology-optimization
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * Create the Kafka Streams Domain Specific Language (DSL) Topology.
     *
     * @param props The streams configuration
     * @return The Topology
     */
    static Topology createTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        DISABLED_VALUE_SERDE.configure(config, false);

        INPUT_KEY_OVERRIDDEN_SERDE.configure(config, true);
        INPUT_VALUE_OVERRIDDEN_SERDE.configure(config, false);

        INPUT_VALUE_REGISTERED_SERDE.configure(config, false);
        INPUT_VALUE_ACTIVE_SERDE.configure(config, false);

        final KTable<String, RegisteredAlarm> registeredTable = builder.table(INPUT_TOPIC_REGISTERED, Consumed.with(INPUT_KEY_REGISTERED_SERDE, INPUT_VALUE_REGISTERED_SERDE));
        final KTable<String, ActiveAlarm> activeTable = builder.table(INPUT_TOPIC_ACTIVE, Consumed.with(INPUT_KEY_ACTIVE_SERDE, INPUT_VALUE_ACTIVE_SERDE));
        //final KStream<OverriddenAlarmKey, OverriddenAlarmValue> overriddenStream = builder.stream(INPUT_TOPIC_OVERRIDDEN, Consumed.with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));
        final KTable<OverriddenAlarmKey, OverriddenAlarmValue> overriddenTable = builder.table(INPUT_TOPIC_OVERRIDDEN, Consumed.with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));




        // I think we want outerJoin to ensure we get updates regardless if other "side" exists
        KTable<String, AlarmStateCalculator> joined =
                registeredTable.outerJoin(activeTable, (registeredAlarm, activeAlarm) -> AlarmStateCalculator.fromRegisteredAndActive(registeredAlarm, activeAlarm));



        KTable<String, DisabledAlarm> disabledTable = overriddenTable.filter((k,v) -> {
                    System.err.println("Key: " + k);
                    System.err.println("Value: " + v);
                    return k.getType() == OverriddenAlarmType.Disabled;
                })
                .groupBy((k,v)-> new KeyValue<>(k.getName(), (DisabledAlarm)v.getMsg()), Grouped.with(Serdes.String(), DISABLED_VALUE_SERDE))
                .aggregate(
                        DisabledAlarm::new,
                        (key, value, obj) -> {
                            return value;
                        },
                        (key, value, obj) -> {
                            return value;
                        },
                        Materialized.with(Serdes.String(), DISABLED_VALUE_SERDE)
                );


        /*KTable<String, DisabledAlarm> disabledTable = overriddenStream
                .filter((k,v) -> {
                    System.err.println("Key: " + k);
                    System.err.println("Value: " + v);
                    return k.getType() == OverriddenAlarmType.Disabled;
                })
                .map((k,v) -> {
                    DisabledAlarm overrideRecord = null;
                    if(v != null) {
                        System.err.println("Msg: " + v.getMsg());
                        if(v.getMsg() instanceof DisabledAlarm) {
                            overrideRecord = (DisabledAlarm) v.getMsg();
                        }
                    }

                    System.err.println("Name (key): " + k.getName());
                    System.err.println("Disabled Record: " + overrideRecord);

                    return new KeyValue<>(k.getName(), overrideRecord);
                })
                .toTable(Materialized
                        .as("disabled-store")
                        .with(INPUT_KEY_REGISTERED_SERDE, DISABLED_VALUE_SERDE));*/

        // Must daisy chain joins...
        KTable<String, AlarmStateCalculator> joined2 =
                disabledTable.outerJoin(joined, (disabledAlarm, alarmState) -> alarmState.addDisabled(disabledAlarm));

        // Now Compute the state
        final KTable<String, String> out = joined2.mapValues(new ValueMapper<AlarmStateCalculator, String>() {
            @Override
            public String apply(AlarmStateCalculator value) {
                String state = "null"; // This should never happen, right?

                if(value != null) {
                    state = value.computeState();
                }

                return state;
            }
        });

        out.toStream().to(OUTPUT_TOPIC, Produced.with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {

        log.info("Starting up AlarmStateProcessor");

        Properties adminProps = getAdminConfig();
        admin = AdminClient.create(adminProps);

        /*Properties registeredProps = getRegisteredConfig();
        registeredTable = new EventSourceTable<>(registeredProps);

        registeredTable.addListener(new EventSourceListener<>() {
            @Override
            public void update(List<EventSourceRecord<String, RegisteredAlarm>> changes) {
            }
        });

        registeredTable.start();*/

        final Properties props = getStreamsConfig();
        final Topology top = createTopology(props);
        streams = new KafkaStreams(top, props);

        streams.start();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                shutdown(null);
            }
        });

        try {
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void shutdown(Exception e) {
        log.info("Shutting Down Streams");
        if(e != null) {
            e.printStackTrace();
        }

        streams.close(); // blocks...

        if(admin != null) {
            admin.close();
        }

        /*if(registeredTable != null) {
            registeredTable.close();
        }*/

        latch.countDown();
    }
}
