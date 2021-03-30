package org.jlab.jaws;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class KTableJoinTest {

    private static final Logger log = LoggerFactory.getLogger(KTableJoinTest.class);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopicA;
    private TestInputTopic<String, String> inputTopicB;
    private TestInputTopic<String, String> inputTopicC;
    private TestInputTopic<String, String> inputTopicD;
    private TestOutputTopic<String, String> outputTopic;
    private Topology top;
    private static final String INPUT_TOPICA_NAME = "topic-a";
    private static final String INPUT_TOPICB_NAME = "topic-b";
    private static final String INPUT_TOPICC_NAME = "topic-c";
    private static final String INPUT_TOPICD_NAME = "topic-d";
    private static final String OUTPUT_TOPIC_NAME = "output";


    private Properties getStreamsConfig() {

        final Properties props = new Properties();
        // Experiment with various options
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        //props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        //props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 100);
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        return props;
    }

    private Topology createTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> tableA = builder.table(INPUT_TOPICA_NAME,
                Consumed.as("Table-A").with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableB = builder.table(INPUT_TOPICB_NAME,
                Consumed.as("Table-B").with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableC = builder.table(INPUT_TOPICC_NAME,
                Consumed.as("Table-C").with(Serdes.String(), Serdes.String()));
        final KTable<String, String> tableD = builder.table(INPUT_TOPICD_NAME,
                Consumed.as("Table-D").with(Serdes.String(), Serdes.String()));

        KTable<String, String> tableAB = tableA.outerJoin(tableB, new ConcatJoiner(), Named.as("A-B"));
        KTable<String, String> tableABC = tableAB.outerJoin(tableC, new ConcatJoiner(), Named.as("A-B-C"));
        KTable<String, String> tableABCD = tableABC.outerJoin(tableD, new ConcatJoiner(), Named.as("A-B-C-D"));

        tableABCD.toStream(Named.as("Intermediate-Stream")).to(OUTPUT_TOPIC_NAME, Produced.as("Enriched").with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    @Before
    public void setup() {

        final Properties streamsConfig = getStreamsConfig();
        top = createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);

        inputTopicA = testDriver.createInputTopic(INPUT_TOPICA_NAME, Serdes.String().serializer(), Serdes.String().serializer());
        inputTopicB = testDriver.createInputTopic(INPUT_TOPICB_NAME, Serdes.String().serializer(), Serdes.String().serializer());
        inputTopicC = testDriver.createInputTopic(INPUT_TOPICC_NAME, Serdes.String().serializer(), Serdes.String().serializer());
        inputTopicD = testDriver.createInputTopic(INPUT_TOPICD_NAME, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    class ConcatJoiner implements ValueJoiner<String, String, String> {
        @Override
        public String apply(String value1, String value2) {
            StringBuilder builder = new StringBuilder();

            if(value1 != null) {
                builder.append(value1);
            }

            if(value2 != null) {
                builder.append(value2);
            }

            String result = builder.toString();

            log.info("ConcatJoin: {} = {} + {}", result, value1, value2);

            return result;
        }
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void dumpTop() {
        System.out.println(top.describe().toString());
    }

    @Test
    public void testA() {
        inputTopicA.pipeInput("key1", "A");
        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();

        Assert.assertEquals(1, outList.size());
        KeyValue<String, String> result = outList.get(0);
        Assert.assertEquals("key1", result.key);
        Assert.assertEquals("A", result.value);
    }

    @Test
    public void testAB() {
        inputTopicA.pipeInput("key1", "A");
        inputTopicB.pipeInput("key1", "B");
        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();
        Assert.assertEquals(2, outList.size());
        KeyValue<String, String> result = outList.get(1);
        Assert.assertEquals("key1", result.key);
        Assert.assertEquals("AB", result.value);
    }

    @Test
    public void testABC() {
        inputTopicA.pipeInput("key1", "A");
        inputTopicB.pipeInput("key1", "B");
        inputTopicC.pipeInput("key1", "C");

        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();
        Assert.assertEquals(3, outList.size());
        KeyValue<String, String> result = outList.get(2);
        Assert.assertEquals("key1", result.key);
        Assert.assertEquals("ABC", result.value);
    }

    @Test
    public void testABCD() {
        inputTopicA.pipeInput("key1", "A");
        inputTopicB.pipeInput("key1", "B");
        inputTopicC.pipeInput("key1", "C");
        inputTopicD.pipeInput("key1", "D");

        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();
        Assert.assertEquals(4, outList.size());
        KeyValue<String, String> result = outList.get(3);
        Assert.assertEquals("key1", result.key);
        Assert.assertEquals("ABCD", result.value);
    }
}
