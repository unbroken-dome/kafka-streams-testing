package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.function.Consumer;


public class CommonTestTopology implements Consumer<StreamsBuilder> {

    @Override
    public void accept(StreamsBuilder builder) {
        builder.stream("plaintext-input", Consumed.with(Serdes.String(), Serdes.String()))
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Serialized.with(Serdes.String(), Serdes.String()))
                .count(Materialized
                        .<String, Long, KeyValueStore<Bytes, byte[]>>as("Counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to("wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
    }
}
