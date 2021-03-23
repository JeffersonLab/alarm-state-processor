package org.jlab.alarms.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jlab.alarms.FilterCommandKey;
import org.jlab.alarms.FilterCommandValue;
import org.jlab.alarms.FilterCommandSerde;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class CommandProducer {
    public static void main(String[] args) {
        String servers = args[0];
        String commandTopic = args[1];
        String outTopic = args[2];
        String unset = args[3];
        String filterName = args[4];
        String alarmNameCsv = args[5];
        String locationCsv = args[6];
        String categoryCsv = args[7];

        FilterCommandKey key = new FilterCommandKey();
        FilterCommandValue value = new FilterCommandValue();

        key.setOutputTopic(outTopic);

        if("true".equals(unset)) {
            value = null;
        } else {
            value.setFilterName(filterName);
            value.setAlarmNames(fromCsv(alarmNameCsv));
            value.setLocations(fromCsv(locationCsv));
            value.setCategories(fromCsv(categoryCsv));
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", FilterCommandSerde.key().serializer().getClass().getName());
        props.put("value.serializer", FilterCommandSerde.value().serializer().getClass().getName());

        try(KafkaProducer<FilterCommandKey, FilterCommandValue> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(commandTopic, key, value));
        }
    }

    static Set<String> fromCsv(String csv) {
        Set<String> result = null;

        if(csv != null && !csv.isEmpty()) {
            String[] resultArray = csv.split(",");
            result = new HashSet<String>(Arrays.asList(resultArray));
        }

        return result;
    }
}