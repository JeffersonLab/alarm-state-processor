package org.jlab.jaws;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.jlab.jaws.entity.*;
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

    public static final SpecificAvroSerde<LatchedAlarm> LATCHED_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<DisabledAlarm> DISABLED_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ShelvedAlarm> SHELVED_VALUE_SERDE = new SpecificAvroSerde<>();

    public static final Serdes.StringSerde OUTPUT_KEY_SERDE = new Serdes.StringSerde();
    public static final Serdes.StringSerde OUTPUT_VALUE_SERDE = new Serdes.StringSerde();

    static KafkaStreams streams;

    final static CountDownLatch latch = new CountDownLatch(1);

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

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disables caching
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        //props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 100);

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

        LATCHED_VALUE_SERDE.configure(config, false);
        DISABLED_VALUE_SERDE.configure(config, false);
        SHELVED_VALUE_SERDE.configure(config, false);

        INPUT_KEY_OVERRIDDEN_SERDE.configure(config, true);
        INPUT_VALUE_OVERRIDDEN_SERDE.configure(config, false);

        INPUT_VALUE_REGISTERED_SERDE.configure(config, false);
        INPUT_VALUE_ACTIVE_SERDE.configure(config, false);

        final KTable<String, RegisteredAlarm> registeredTable = builder.table(INPUT_TOPIC_REGISTERED,
                Consumed.as("Registered-Table").with(INPUT_KEY_REGISTERED_SERDE, INPUT_VALUE_REGISTERED_SERDE));
        final KTable<String, ActiveAlarm> activeTable = builder.table(INPUT_TOPIC_ACTIVE,
                Consumed.as("Active-Table").with(INPUT_KEY_ACTIVE_SERDE, INPUT_VALUE_ACTIVE_SERDE));
        //final KTable<OverriddenAlarmKey, OverriddenAlarmValue> overriddenTable = builder.table(INPUT_TOPIC_OVERRIDDEN,
        //        Consumed.as("Overridden-Table").with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));

        final KStream<OverriddenAlarmKey, OverriddenAlarmValue> overriddenStream = builder.stream(INPUT_TOPIC_OVERRIDDEN,
                Consumed.as("Overridden-Stream").with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));

        @SuppressWarnings("unchecked")
        KStream<OverriddenAlarmKey, OverriddenAlarmValue>[] overrideArray = overriddenStream.branch(
                Named.as("Split-Overrides"),
                (key, value) -> key.getType() == OverriddenAlarmType.Disabled,
                (key, value) -> key.getType() == OverriddenAlarmType.Shelved,
                (key, value) -> key.getType() == OverriddenAlarmType.Latched
        );

        KStream<String, DisabledAlarm> disabledStream = overrideArray[0]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, DisabledAlarm>>)
                        (key, value) -> new KeyValue<>(key.getName(), toDisabledAlarm(value)),
                Named.as("Disabled-Map"));

        KStream<String, ShelvedAlarm> shelvedStream = overrideArray[1]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, ShelvedAlarm>>)
                        (key, value) -> new KeyValue<>(key.getName(), toShelvedAlarm(value)),
                Named.as("Shelved-Map"));

        KStream<String, LatchedAlarm> latchedStream = overrideArray[2]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, LatchedAlarm>>)
                                (key, value) -> new KeyValue<>(key.getName(), toLatchedAlarm(value)),
                        Named.as("Latched-Map"));

        KTable<String, DisabledAlarm> disabledTable = disabledStream.toTable(Materialized.as("Disabled-Table")
                .with(Serdes.String(), DISABLED_VALUE_SERDE));

        KTable<String, ShelvedAlarm> shelvedTable = shelvedStream.toTable(Materialized.as("Shelved-Table")
                .with(Serdes.String(), SHELVED_VALUE_SERDE));

        KTable<String, LatchedAlarm> latchedTable = latchedStream.toTable(Materialized.as("Latched-Table")
                .with(Serdes.String(), LATCHED_VALUE_SERDE));

        // I think we want outerJoin to ensure we get updates regardless if other "side" exists
        KTable<String, AlarmStateCalculator> joined = registeredTable
                .outerJoin(activeTable,
                        (registeredAlarm, activeAlarm) ->
                                AlarmStateCalculator.fromRegisteredAndActive(registeredAlarm, activeAlarm),
                        Named.as("Registered-and-Active-Table"));

        // Daisy chain joins
        KTable<String, AlarmStateCalculator> joined2 = disabledTable
                .outerJoin(joined, (disabledAlarm, alarmState) ->
                        alarmState.setDisabled(disabledAlarm),
                        Named.as("Plus-Disabled"));

        // Daisy chain joins
        KTable<String, AlarmStateCalculator> joined3 = shelvedTable
                .outerJoin(joined2, (shelvedAlarm, alarmState) ->
                        alarmState.setShelved(shelvedAlarm),
                        Named.as("Plus-Shelved"));

        // Daisy chain joins
        KTable<String, AlarmStateCalculator> joined4 = latchedTable
                .outerJoin(joined3, (latchedAlarm, alarmState) ->
                                alarmState.setLatched(latchedAlarm),
                        Named.as("Plus-Latched"));

        // Assign names for AlarmStateCalculator to access (debugging)
        KTable<String, AlarmStateCalculator> named = joined4
                .transformValues(new MsgTransformerFactory(), Named.as("Key-Added-to-Value"));

        // Now Compute the state
        final KTable<String, String> out = named.mapValues(new ValueMapper<AlarmStateCalculator, String>() {
            @Override
            public String apply(AlarmStateCalculator value) {
                String state = "null"; // This should never happen, right?

                if(value != null) {
                    state = value.computeState();
                }

                return state;
            }
        }, Named.as("Compute-State"));

        out.toStream(Named.as("State-Stream")).to(OUTPUT_TOPIC, Produced.as("State-Sink")
                .with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    private static LatchedAlarm toLatchedAlarm(OverriddenAlarmValue value) {
        LatchedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof LatchedAlarm) {
            alarm = (LatchedAlarm) value.getMsg();
        }

        return alarm;
    }

    private static DisabledAlarm toDisabledAlarm(OverriddenAlarmValue value) {
        DisabledAlarm alarm = null;

        if(value != null && value.getMsg() instanceof DisabledAlarm) {
            alarm = (DisabledAlarm)value.getMsg();
        }

        return alarm;
    }

    private static ShelvedAlarm toShelvedAlarm(OverriddenAlarmValue value) {
        ShelvedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof ShelvedAlarm) {
            alarm = (ShelvedAlarm)value.getMsg();
        }

        return alarm;
    }

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {

        log.info("Starting up AlarmStateProcessor");

        final Properties props = getStreamsConfig();
        final Topology top = createTopology(props);
        streams = new KafkaStreams(top, props);

        // View output at: https://zz85.github.io/kafka-streams-viz/
        log.info(top.describe().toString());

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

        latch.countDown();
    }

    private static final class MsgTransformerFactory implements ValueTransformerWithKeySupplier<String, AlarmStateCalculator, AlarmStateCalculator> {


        @Override
        public ValueTransformerWithKey<String, AlarmStateCalculator, AlarmStateCalculator> get() {
            return new ValueTransformerWithKey<>() {
                @Override
                public void init(ProcessorContext context) {

                }

                @Override
                public AlarmStateCalculator transform(String readOnlyKey, AlarmStateCalculator value) {
                    value.setAlarmName(readOnlyKey);

                    return value;
                }

                @Override
                public void close() {

                }
            };
        }
    }
}
