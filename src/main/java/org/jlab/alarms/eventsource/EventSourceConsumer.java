package org.jlab.alarms.eventsource;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class EventSourceConsumer<K, V> extends Thread implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(EventSourceConsumer.class);

    private final KafkaConsumer<K, V> consumer;
    private final EventSourceConfig config;
    private final Set<EventSourceListener<K, V>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private long endOffset = 0;
    private boolean endReached = false;

    private AtomicReference<CONSUMER_STATE> consumerState = new AtomicReference<>(CONSUMER_STATE.INITIALIZING);

    // Both current state and changes since last listener notification are tracked
    private final HashMap<K, EventSourceRecord<K, V>> state = new HashMap<>();
    private final List<EventSourceRecord<K, V>> changes = new ArrayList<>();

    public EventSourceConsumer(Properties props) {

        config = new EventSourceConfig(props);

        // Not sure if there is a better way to get configs (with defaults) from EventSourceConfig into a
        // Properties object (or Map) for KafkaConsumer - we manually copy values over into a new clean Properties.
        // Tried the following without success:
        // - if you use config.valuesWithPrefixOverride() to obtain consumer props it will compile, but serialization
        // may fail at runtime w/ClassCastException! (I guess stuff is mangled from String to Objects or something)
        // - if you simply pass the constructor argument above "props" along to KafkaConsumer, the defaults for missing
        // values won't be set.
        Properties consumerProps = new Properties();

        // Pass values in as is from user (without defaults); then next we'll ensure defaults are used if needed
        // Note: using new Properties(props) does NOT work as that sets the defaults field inside the Properties object,
        // which are not carried over later
        // inside the KafkaConsumer constructor when it also creates a new Properties and uses putAll().
        consumerProps.putAll(props);

        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS));
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_GROUP));
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER));
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER));

        // Deserializer specific configs are passed in via putAll(props) and don't have defaults in EventSourceConfig
        // Examples:
        // - KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG
        // - KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG


        consumer = new KafkaConsumer<K, V>(consumerProps);
    }

    public void addListener(EventSourceListener<K, V> listener) {
        listeners.add(listener);
    }

    public void removeListener(EventSourceListener<K, V> listener) {
        listeners.remove(listener);
    }

    @Override
    public void run() {
        boolean transitioned = consumerState.compareAndSet(CONSUMER_STATE.INITIALIZING, CONSUMER_STATE.RUNNING);

        if(!transitioned) {
            log.debug("We must already be closed!");
        }

        if(transitioned) { // Only allow the first time!
            try {
                init();
                monitorChanges();
            } catch(WakeupException e) {
                // We expect this when CLOSED (since we call consumer.wakeup()), else throw
                if(consumerState.get() != CONSUMER_STATE.CLOSED) throw e;
            } finally {
                consumer.close();
            }
        }
    }

    private void init() {
        log.debug("subscribing to topic: {}", config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC));

        consumer.subscribe(Collections.singletonList(config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC)), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.debug("Seeking to beginning of topic");

                if(partitions.size() != 1) {
                    throw new IllegalStateException("An Event Sourced topic must have exactly one partition else message ordering is not guaranteed");
                }

                consumer.seekToBeginning(partitions);

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

                System.out.println("endOffsets: " + endOffsets);

                endOffset = endOffsets.get(partitions.iterator().next()); // Exactly one partition verified above

                if(endOffset == 0) {
                    log.debug("Empty topic to begin with");
                    endReached = true;
                }
            }
        });

        // Note: first poll triggers seek to beginning (so some empty polls are expected)
        int emptyPollCount = 0;

        while(!endReached && consumerState.get() == CONSUMER_STATE.RUNNING) {

            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(config.getLong(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS)));

            log.debug("found " + records.count() + " records");

            for (ConsumerRecord<K, V> record : records) {
                updateState(record);

                log.debug("Looking for last index: {}, found: {}", endOffset, record.offset() + 1);

                if(record.offset() + 1 == endOffset) {
                    log.debug("end of partition {} reached", record.partition());
                    endReached = true;
                }
            }

            if(records.count() == 0) {
                emptyPollCount++;
            }

            if(emptyPollCount > 10) {
                // Scenario where topic compaction is not configured properly.
                // Last message (highest endOffset) deleted before consumer connected (so never received)!
                // It is also possible server just isn't delivering messages timely, so this may be bad check...
                throw new RuntimeException("Took too long to obtain initial list; verify topic compact policy!");
            }
        }

        // Toss out old messages on first update
        changes.clear();
        changes.addAll(state.values());
        notifyListeners(); // we always notify even if changes is empty - this tells clients initial state

        log.debug("Done with EventSourceConsumer init");
    }

    private void monitorChanges() {
        // We wait until changes have settled to avoid notifying listeners of individual changes
        // We use a simple strategy of waiting for a single poll without changes to flush
        // But we won't let changes build up too long either so we check for max poll with changes
        int pollsWithChangesSinceLastFlush = 0;
        boolean hasChanges = false;

        while(consumerState.get() == CONSUMER_STATE.RUNNING) {
            log.debug("polling for changes ({})", config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC));
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(config.getLong(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS)));

            if (records.count() > 0) { // We have changes
                for(ConsumerRecord<K, V> record: records) {
                    updateState(record);
                }

                log.debug("Change in topic: request update once settled");
                hasChanges = true;
            } else {
                if(hasChanges) {
                    log.debug("Flushing changes since we've settled (we had a poll with no changes)");
                    notifyListeners();
                    hasChanges = false;
                    pollsWithChangesSinceLastFlush = 0;
                }
            }

            // This is an escape hatch in case poll consistently returns changes; otherwise we'd never flush!
            if(pollsWithChangesSinceLastFlush >= config.getLong(EventSourceConfig.EVENT_SOURCE_MAX_POLL_BEFORE_FLUSH)) {
                log.debug("Flushing changes due to max poll with changes");
                notifyListeners();
                hasChanges = false;
                pollsWithChangesSinceLastFlush = 0;
            }

            if(hasChanges) {
                pollsWithChangesSinceLastFlush++;
            }
        }
    }

    private void notifyListeners() {
        for(EventSourceListener<K, V> listener: listeners) {
            listener.update(new ArrayList<>(changes)); // Safer to return new List
        }
        changes.clear();
    }

    private void updateState(ConsumerRecord<K, V> record) {
        log.debug("State update: {}={}", record.key(), record.value());

        EventSourceRecord<K, V> esr = new EventSourceRecord<>(record.key(), record.value());
        changes.add(esr);

        if(record.value() == null) {
            log.debug("Removing record: {}", record.key());
            state.remove(record.key());
        } else {
            log.debug("Adding record: {}", record.key());
            state.put(record.key(), esr);
        }
    }

    @Override
    public void close() {
        CONSUMER_STATE previousState = consumerState.getAndSet(CONSUMER_STATE.CLOSED);

        if(previousState == CONSUMER_STATE.INITIALIZING) { // start() never called!
            consumer.close();
        } else {
            consumer.wakeup(); // tap on shoulder and it'll eventually notice consumer state now CLOSED
        }
    }

    private enum CONSUMER_STATE {
        INITIALIZING, RUNNING, CLOSED
    }
}
