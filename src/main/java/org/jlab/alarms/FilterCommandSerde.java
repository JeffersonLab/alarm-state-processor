package org.jlab.alarms;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.jlab.alarms.serde.JsonDeserializer;
import org.jlab.alarms.serde.JsonSerializer;

public final class FilterCommandSerde {
    // We require a no-args constructor so we create a new class that wraps JsonDeserializer
    static public final class FilterCommandKeyDeserializer extends JsonDeserializer<FilterCommandKey> {

        public FilterCommandKeyDeserializer() {
            super(FilterCommandKey.class);
        }
    }

    static public final class FilterCommandValueDeserializer extends JsonDeserializer<FilterCommandValue> {

        public FilterCommandValueDeserializer() {
            super(FilterCommandValue.class);
        }
    }


    static public final class FilterCommandKeySerde
            extends Serdes.WrapperSerde<FilterCommandKey> {
        public FilterCommandKeySerde() {
            super(new JsonSerializer<>(),
                    new FilterCommandKeyDeserializer());
        }
    }

    static public final class FilterCommandValueSerde
            extends Serdes.WrapperSerde<FilterCommandValue> {
        public FilterCommandValueSerde() {
            super(new JsonSerializer<>(),
                    new FilterCommandValueDeserializer());
        }
    }

    public static Serde<FilterCommandKey> key() {
        return new FilterCommandKeySerde();
    }

    public static Serde<FilterCommandValue> value() {
        return new FilterCommandValueSerde();
    }
}