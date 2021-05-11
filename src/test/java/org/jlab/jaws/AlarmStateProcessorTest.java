package org.jlab.jaws;

import org.apache.kafka.streams.*;
import org.jlab.jaws.entity.*;
import org.junit.*;

import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmStateProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, RegisteredAlarm> registeredInputTopic;
    private TestInputTopic<String, ActiveAlarm> activeInputTopic;
    private TestInputTopic<OverriddenAlarmKey, OverriddenAlarmValue> overriddenInputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private Topology top;
    private RegisteredAlarm registeredAlarm1 = new RegisteredAlarm();
    private RegisteredAlarm registeredAlarm2 = new RegisteredAlarm();
    private ActiveAlarm activeAlarm1 = new ActiveAlarm();
    private ActiveAlarm activeAlarm2 = new ActiveAlarm();


    @Before
    public void setup() {
        final Properties streamsConfig = AlarmStateProcessor.getStreamsConfig();
        streamsConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        top = AlarmStateProcessor.createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);

        registeredInputTopic = testDriver.createInputTopic(AlarmStateProcessor.INPUT_TOPIC_REGISTERED, AlarmStateProcessor.INPUT_KEY_REGISTERED_SERDE.serializer(), AlarmStateProcessor.INPUT_VALUE_REGISTERED_SERDE.serializer());
        activeInputTopic = testDriver.createInputTopic(AlarmStateProcessor.INPUT_TOPIC_ACTIVE, AlarmStateProcessor.INPUT_KEY_ACTIVE_SERDE.serializer(), AlarmStateProcessor.INPUT_VALUE_ACTIVE_SERDE.serializer());
        overriddenInputTopic = testDriver.createInputTopic(AlarmStateProcessor.INPUT_TOPIC_OVERRIDDEN, AlarmStateProcessor.INPUT_KEY_OVERRIDDEN_SERDE.serializer(), AlarmStateProcessor.INPUT_VALUE_OVERRIDDEN_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(AlarmStateProcessor.OUTPUT_TOPIC, AlarmStateProcessor.OUTPUT_KEY_SERDE.deserializer(), AlarmStateProcessor.OUTPUT_VALUE_SERDE.deserializer());

        registeredAlarm1.setClass$(AlarmClass.Base_Class);
        registeredAlarm1.setPriority(AlarmPriority.P3_MINOR);
        registeredAlarm1.setCategory(AlarmCategory.Aperture);
        registeredAlarm1.setLocation(AlarmLocation.A1);
        registeredAlarm1.setProducer(new SimpleProducer());
        registeredAlarm1.setRationale("Testing");
        registeredAlarm1.setCorrectiveaction("Call expert");
        registeredAlarm1.setFilterable(false);
        registeredAlarm1.setLatching(false);
        registeredAlarm1.setMaskedby(null);
        registeredAlarm1.setPointofcontactusername("tester");
        registeredAlarm1.setScreenpath("/");

        activeAlarm1.setMsg(new SimpleAlarming());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Ignore
    @Test
    public void dumpTopology() {
        // View output at: https://zz85.github.io/kafka-streams-viz/
        System.out.println(top.describe().toString());
    }

    @Test
    public void testNormal() {
        registeredInputTopic.pipeInput("alarm1", registeredAlarm1);
        KeyValue<String, String> result = outputTopic.readKeyValuesToList().get(0);
        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals("Normal", result.value);
    }

    @Test
    public void testActive() {
        registeredInputTopic.pipeInput("alarm1", registeredAlarm1);
        activeInputTopic.pipeInput("alarm1", activeAlarm1);
        KeyValue<String, String> result = outputTopic.readKeyValuesToList().get(1);
        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals("Active", result.value);
    }

    @Test
    public void testLatched() {
        registeredInputTopic.pipeInput("alarm1", registeredAlarm1);
        activeInputTopic.pipeInput("alarm1", activeAlarm1);

        OverriddenAlarmValue overriddenAlarmValue1 = new OverriddenAlarmValue();
        LatchedAlarm latchedAlarm = new LatchedAlarm();
        overriddenAlarmValue1.setMsg(latchedAlarm);
        overriddenInputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Latched), overriddenAlarmValue1);

        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();
        Assert.assertEquals(3, outList.size());
        KeyValue<String, String> result = outList.get(2);
        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals("Latched", result.value);
    }

    @Test
    public void testDisabled() {
        registeredInputTopic.pipeInput("alarm1", registeredAlarm1);
        activeInputTopic.pipeInput("alarm1", activeAlarm1);

        OverriddenAlarmValue overriddenAlarmValue1 = new OverriddenAlarmValue();
        LatchedAlarm latchedAlarm = new LatchedAlarm();
        overriddenAlarmValue1.setMsg(latchedAlarm);
        overriddenInputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Latched), overriddenAlarmValue1);

        OverriddenAlarmValue overriddenAlarmValue2 = new OverriddenAlarmValue();
        DisabledAlarm disabledAlarm = new DisabledAlarm();
        disabledAlarm.setComments("Testing");
        overriddenAlarmValue2.setMsg(disabledAlarm);
        overriddenInputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), overriddenAlarmValue2);

        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();
        Assert.assertEquals(4, outList.size());
        KeyValue<String, String> result = outList.get(3);
        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals("Disabled", result.value);
    }

    @Test
    public void testUnDisabled() {
        registeredInputTopic.pipeInput("alarm1", registeredAlarm1);
        activeInputTopic.pipeInput("alarm1", activeAlarm1);

        OverriddenAlarmValue overriddenAlarmValue1 = new OverriddenAlarmValue();
        LatchedAlarm latchedAlarm = new LatchedAlarm();
        overriddenAlarmValue1.setMsg(latchedAlarm);
        overriddenInputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Latched), overriddenAlarmValue1);

        OverriddenAlarmValue overriddenAlarmValue2 = new OverriddenAlarmValue();
        DisabledAlarm disabledAlarm = new DisabledAlarm();
        disabledAlarm.setComments("Testing");
        overriddenAlarmValue2.setMsg(disabledAlarm);
        overriddenInputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), overriddenAlarmValue2);


        overriddenInputTopic.pipeInput(new OverriddenAlarmKey("alarm1", OverriddenAlarmType.Disabled), null);

        List<KeyValue<String, String>> outList = outputTopic.readKeyValuesToList();
        Assert.assertEquals(5, outList.size());
        KeyValue<String, String> result = outList.get(4);
        Assert.assertEquals("alarm1", result.key);
        Assert.assertEquals("Latched", result.value);
    }
}
