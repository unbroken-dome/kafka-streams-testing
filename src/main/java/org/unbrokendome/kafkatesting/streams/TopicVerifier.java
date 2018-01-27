package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;


@SuppressWarnings("UnusedReturnValue")
public class TopicVerifier<K, V> {

    private final ProcessorTopologyTestDriver testDriver;
    private final String topic;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private long lastOffset = -1L;


    TopicVerifier(ProcessorTopologyTestDriver testDriver, String topic,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this.testDriver = testDriver;
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }


    private KeyValue<K, V> readNext() {
        ProducerRecord<K, V> record = testDriver.readOutput(topic, keyDeserializer, valueDeserializer);
        if (record == null) {
            return null;
        }

        lastOffset++;
        return new KeyValue<>(record.key(), record.value());
    }


    public TopicVerifier<K, V> expectNext(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {

        KeyValue<K, V> keyValue = readNext();
        MatcherAssert.assertThat("Topic \"" + topic + "\" record #" + lastOffset,
                keyValue,
                new KeyValueMatcher<>(keyMatcher, valueMatcher));
        return this;
    }


    public TopicVerifier<K, V> expectComplete() {
        ProducerRecord<K, V> record = testDriver.readOutput(topic, keyDeserializer, valueDeserializer);
        MatcherAssert.assertThat("Topic has no more records", record, nullValue());
        return this;
    }


    public List<KeyValue<K, V>> toList() {
        List<KeyValue<K, V>> list = new ArrayList<>();

        KeyValue<K, V> keyValue;
        while ((keyValue = readNext()) != null) {
            list.add(keyValue);
        }

        return list;
    }


    public Map<K, V> toMap() {
        Map<K, V> map = new LinkedHashMap<>();

        KeyValue<K, V> keyValue;
        while ((keyValue = readNext()) != null) {
            map.put(keyValue.key, keyValue.value);
        }

        return map;
    }
}
