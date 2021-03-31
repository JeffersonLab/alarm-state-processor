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

    public static final SpecificAvroSerde<AlarmStateCriteria> CRITERIA_VALUE_SERDE = new SpecificAvroSerde<>();
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
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registry);

        // https://stackoverflow.com/questions/57164133/kafka-stream-topology-optimization
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disables caching
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        //props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 100);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static KTable<String, AlarmStateCriteria> getOverriddenCriteriaViaGroupBy(StreamsBuilder builder) {
        final KTable<OverriddenAlarmKey, OverriddenAlarmValue> overriddenTable = builder.table(INPUT_TOPIC_OVERRIDDEN,
                Consumed.as("Overridden-Table").with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));

        final KTable<String, AlarmStateCriteria> overrideCriteriaTable = overriddenTable
                .groupBy((key, value) -> groupOverrideCriteria(key, value), Grouped.as("Grouped-Overrides")
                        .with(Serdes.String(), CRITERIA_VALUE_SERDE))
                .aggregate(
                        () -> new AlarmStateCriteria(),
                        (key, newValue, aggregate) -> {
                            AlarmStateCalculator calculator = new AlarmStateCalculator();
                            calculator.append(aggregate);
                            calculator.append(newValue);
                            return calculator.getCriteria();
                        },
                        (key, oldValue, aggregate) -> {
                            AlarmStateCalculator calculator = new AlarmStateCalculator();
                            calculator.append(aggregate);
                            calculator.remove(oldValue);
                            return calculator.getCriteria();
                        },
                        Materialized.as("Override-Criteria-Table").with(Serdes.String(), CRITERIA_VALUE_SERDE));

        return overrideCriteriaTable;
    }

    private static KeyValue<String, AlarmStateCriteria> groupOverrideCriteria(OverriddenAlarmKey key, OverriddenAlarmValue value) {
        AlarmStateCriteria criteria;

        switch(key.getType()) {
            case Latched:
                criteria = toLatchedCriteria(value);
                break;
            case OffDelayed:
                criteria = toOffDelayedCriteria(value);
                break;
            case Shelved:
                criteria = toShelvedCriteria(value);
                break;
            case OnDelayed:
                criteria = toOnDelayedCriteria(value);
                break;
            case Masked:
                criteria = toMaskedCriteria(value);
                break;
            case Filtered:
                criteria = toFilteredCriteria(value);
                break;
            case Disabled:
                criteria = toDisabledCriteria(value);
                break;
            default:
                throw new RuntimeException("Unknown OverriddenAlarmType: " + key.getType());
        }

        return new KeyValue<>(key.getName(), criteria);
    }

    private static KTable<String, AlarmStateCriteria> getOverriddenCriteriaViaBranchAndMapAndJoin(StreamsBuilder builder) {
        final KStream<OverriddenAlarmKey, OverriddenAlarmValue> overriddenStream = builder.stream(INPUT_TOPIC_OVERRIDDEN,
                Consumed.as("Overridden-Stream").with(INPUT_KEY_OVERRIDDEN_SERDE, INPUT_VALUE_OVERRIDDEN_SERDE));

        @SuppressWarnings("unchecked")
        KStream<OverriddenAlarmKey, OverriddenAlarmValue>[] overrideArray = overriddenStream.branch(
                Named.as("Split-Overrides"),
                (key, value) -> key.getType() == OverriddenAlarmType.Latched,
                (key, value) -> key.getType() == OverriddenAlarmType.OffDelayed,
                (key, value) -> key.getType() == OverriddenAlarmType.Shelved,
                (key, value) -> key.getType() == OverriddenAlarmType.OnDelayed,
                (key, value) -> key.getType() == OverriddenAlarmType.Masked,
                (key, value) -> key.getType() == OverriddenAlarmType.Filtered,
                (key, value) -> key.getType() == OverriddenAlarmType.Disabled
        );

        // Map overrides to AlarmStateCriteria
        KStream<String, AlarmStateCriteria> latchedStream = overrideArray[0]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toLatchedCriteria(value)),
                        Named.as("Latched-Map"));

        KStream<String, AlarmStateCriteria> offDelayedStream = overrideArray[1]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toOffDelayedCriteria(value)),
                        Named.as("Off-Delayed-Map"));

        KStream<String, AlarmStateCriteria> shelvedStream = overrideArray[2]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toShelvedCriteria(value)),
                        Named.as("Shelved-Map"));

        KStream<String, AlarmStateCriteria> onDelayedStream = overrideArray[3]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toOnDelayedCriteria(value)),
                        Named.as("On-Delayed-Map"));

        KStream<String, AlarmStateCriteria> maskedStream = overrideArray[4]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toMaskedCriteria(value)),
                        Named.as("Masked-Map"));

        KStream<String, AlarmStateCriteria> filteredStream = overrideArray[5]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toFilteredCriteria(value)),
                        Named.as("Filtered-Map"));

        KStream<String, AlarmStateCriteria> disabledStream = overrideArray[6]
                .map((KeyValueMapper<OverriddenAlarmKey, OverriddenAlarmValue, KeyValue<String, AlarmStateCriteria>>)
                                (key, value) -> new KeyValue<>(key.getName(), toDisabledCriteria(value)),
                        Named.as("Disabled-Map"));


        // Materialize KTables from override criteria streams
        KTable<String, AlarmStateCriteria> latchedTable = latchedStream.toTable(Materialized.as("Latched-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        KTable<String, AlarmStateCriteria> offDelayedTable = offDelayedStream.toTable(Materialized.as("OffDelayed-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        KTable<String, AlarmStateCriteria> shelvedTable = shelvedStream.toTable(Materialized.as("Shelved-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        KTable<String, AlarmStateCriteria> onDelayedTable = onDelayedStream.toTable(Materialized.as("OnDelayed-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        KTable<String, AlarmStateCriteria> maskedTable = maskedStream.toTable(Materialized.as("Masked-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        KTable<String, AlarmStateCriteria> filteredTable = filteredStream.toTable(Materialized.as("Filtered-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        KTable<String, AlarmStateCriteria> disabledTable = disabledStream.toTable(Materialized.as("Disabled-Table")
                .with(Serdes.String(), CRITERIA_VALUE_SERDE));

        // Start joining!
        KTable<String, AlarmStateCriteria> latchedAndOffDelayed = latchedTable
                .outerJoin(offDelayedTable, new StateCriteriaJoiner(),
                        Named.as("Latched-And-OffDelayed"));

        // Daisy chain joins
        KTable<String, AlarmStateCriteria> plusShelving =latchedAndOffDelayed
                .outerJoin(shelvedTable, new StateCriteriaJoiner(),
                        Named.as("Plus-Shelved"));

        // Daisy chain joins
        KTable<String, AlarmStateCriteria> plusOnDelayed = plusShelving
                .outerJoin(onDelayedTable, new StateCriteriaJoiner(),
                        Named.as("Plus-OnDelayed"));

        // Daisy chain joins
        KTable<String, AlarmStateCriteria> plusMasked = plusOnDelayed
                .outerJoin(maskedTable, new StateCriteriaJoiner(),
                        Named.as("Plus-Masked"));

        // Daisy chain joins
        KTable<String, AlarmStateCriteria> plusFiltered = plusMasked
                .outerJoin(filteredTable, new StateCriteriaJoiner(),
                        Named.as("Plus-Filtered"));

        // Daisy chain joins
        KTable<String, AlarmStateCriteria> overrideCriteriaTable = plusFiltered
                .outerJoin(disabledTable, new StateCriteriaJoiner(),
                        Named.as("Plus-Disabled"));

        return overrideCriteriaTable;
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

        CRITERIA_VALUE_SERDE.configure(config, false);
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



        // Map registered and active kTables to AlarmStateCriteria tables
        KTable<String, AlarmStateCriteria> registeredCritiera = registeredTable
                .mapValues(value -> toRegisteredCriteria(value));

        KTable<String, AlarmStateCriteria> activeCritiera = activeTable
                .mapValues(value -> toActiveCriteria(value));


        KTable<String, AlarmStateCriteria> overriddenCriteria =
                //getOverriddenCriteriaViaBranchAndMapAndJoin(builder);
                getOverriddenCriteriaViaGroupBy(builder);


        // Now we start joining all the streams together

        // I think we want outerJoin to ensure we get updates regardless if other "side" exists
        KTable<String, AlarmStateCriteria> registeredAndActive = registeredCritiera
                .outerJoin(activeCritiera,
                        new StateCriteriaJoiner(),
                        Named.as("Registered-and-Active-Table"));

        // Daisy chain joins
        KTable<String, AlarmStateCriteria> plusOverrides = registeredAndActive
                .outerJoin(overriddenCriteria, new StateCriteriaJoiner(),
                        Named.as("Plus-Latched"));




        // Assign alarm names in keys into value part of record (AlarmStateCriteria) - this is really only be debugging
        KTable<String, AlarmStateCriteria> keyInValueStream = plusOverrides
                .transformValues(new MsgTransformerFactory(), Named.as("Key-Added-to-Value"));

        // Now Compute the state
        final KTable<String, String> out = keyInValueStream.mapValues(new ValueMapper<AlarmStateCriteria, String>() {
            @Override
            public String apply(AlarmStateCriteria value) {
                String state = "null"; // This should never happen, right?

                if(value != null) {
                    AlarmStateCalculator calculator = new AlarmStateCalculator();
                    calculator.append(value);
                    state = calculator.computeState();
                }

                return state;
            }
        }, Named.as("Compute-State"));

        out.toStream(Named.as("State-Stream")).to(OUTPUT_TOPIC, Produced.as("State-Sink")
                .with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    static class StateCriteriaJoiner implements ValueJoiner<AlarmStateCriteria, AlarmStateCriteria, AlarmStateCriteria> {
        @Override
        public AlarmStateCriteria apply(AlarmStateCriteria value1, AlarmStateCriteria value2) {
            AlarmStateCalculator calculator = new AlarmStateCalculator();

            if(value1 != null) {
                calculator.append(value1);
            }

            if(value2 != null) {
                calculator.append(value2);
            }

            AlarmStateCriteria result = calculator.getCriteria();

            log.info("StateCriteriaJoin: {} = {} + {}", result, value1, value2);

            return result;
        }
    }

    private static AlarmStateCriteria toRegisteredCriteria(RegisteredAlarm value) {
        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setRegistered(value != null);

        return criteria;
    }

    private static AlarmStateCriteria toActiveCriteria(ActiveAlarm value) {
        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setActive(value != null);

        return criteria;
    }

    private static AlarmStateCriteria toLatchedCriteria(OverriddenAlarmValue value) {
        LatchedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof LatchedAlarm) {
            alarm = (LatchedAlarm) value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setLatched(alarm != null);

        return criteria;
    }

    private static AlarmStateCriteria toOffDelayedCriteria(OverriddenAlarmValue value) {
        OffDelayedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof OffDelayedAlarm) {
            alarm = (OffDelayedAlarm) value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setOffDelayed(alarm != null);

        return criteria;
    }

    private static AlarmStateCriteria toShelvedCriteria(OverriddenAlarmValue value) {
        ShelvedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof ShelvedAlarm) {
            alarm = (ShelvedAlarm)value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();

        if(alarm != null) {
            if(alarm.getOneshot()) {
                criteria.setContinuousShelved(true);
            } else {
                criteria.setOneshotShelved(true);
            }
        }

        return criteria;
    }

    private static AlarmStateCriteria toOnDelayedCriteria(OverriddenAlarmValue value) {
        OnDelayedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof OnDelayedAlarm) {
            alarm = (OnDelayedAlarm) value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setOnDelayed(alarm != null);

        return criteria;
    }

    private static AlarmStateCriteria toMaskedCriteria(OverriddenAlarmValue value) {
        MaskedAlarm alarm = null;

        if(value != null && value.getMsg() instanceof MaskedAlarm) {
            alarm = (MaskedAlarm)value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setMasked(alarm != null);

        return criteria;
    }

    private static AlarmStateCriteria toFilteredCriteria(OverriddenAlarmValue value) {
        FilteredAlarm alarm = null;

        if(value != null && value.getMsg() instanceof FilteredAlarm) {
            alarm = (FilteredAlarm)value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setFiltered(alarm != null);

        return criteria;
    }

    private static AlarmStateCriteria toDisabledCriteria(OverriddenAlarmValue value) {
        DisabledAlarm alarm = null;

        if(value != null && value.getMsg() instanceof DisabledAlarm) {
            alarm = (DisabledAlarm)value.getMsg();
        }

        AlarmStateCriteria criteria = new AlarmStateCriteria();
        criteria.setDisabled(alarm != null);

        return criteria;
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

    private static final class MsgTransformerFactory implements ValueTransformerWithKeySupplier<String, AlarmStateCriteria, AlarmStateCriteria> {


        @Override
        public ValueTransformerWithKey<String, AlarmStateCriteria, AlarmStateCriteria> get() {
            return new ValueTransformerWithKey<>() {
                @Override
                public void init(ProcessorContext context) {

                }

                @Override
                public AlarmStateCriteria transform(String readOnlyKey, AlarmStateCriteria value) {
                    value.setName(readOnlyKey);

                    return value;
                }

                @Override
                public void close() {

                }
            };
        }
    }
}
