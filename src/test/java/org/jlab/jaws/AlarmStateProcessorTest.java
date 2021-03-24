package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.eventsource.EventSourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmStateProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @Before
    public void setup() {

        final String outTopicName = "alarms-filter-test";

        final Properties streamsConfig = AlarmStateProcessor.getStreamsConfig();
        streamsConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = AlarmStateProcessor.createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }
}
