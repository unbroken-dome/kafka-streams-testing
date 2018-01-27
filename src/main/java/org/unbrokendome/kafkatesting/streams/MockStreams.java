package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@SuppressWarnings({ "unchecked", "UnusedReturnValue" })
public interface MockStreams {

    <K, V> TopicVerifier<K, V> topic(String topic, Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer);


    default <K, V> TopicVerifier<K, V> topic(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return topic(topic, keySerde.deserializer(), valueSerde.deserializer());
    }


    <K, V> TableVerifier<K, V> table(String stateStoreName);


    MockStreams initStateStore(String name, Consumer<StateStore> initializer);


    <K, V> MockStreams initKeyValueStore(String name, Consumer<KeyValueStore<K, V>> initializer);


    default <K, V> MockStreams initKeyValueStore(String name, List<KeyValue<K, V>> entries) {
        return this.<K, V>initKeyValueStore(name,
                store -> store.putAll(entries));
    }


    default <K, V> MockStreams initKeyValueStore(String name, Map<K, V> entries) {
        return initKeyValueStore(name,
                entries.entrySet().stream()
                        .map(entry -> new KeyValue<>(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList()));
    }


    <K, V> Inputs<K, V> input(String topic, Serializer<? super K> keySerializer,
                              Serializer<? super V> valueSerializer);


    default <K, V> Inputs<K, V> input(String topic, Serde<? super K> keySerde,
                              Serde<? super V> valueSerde) {
        return input(topic, keySerde.serializer(), valueSerde.serializer());
    }


    default <K, V> MockStreams input(String topic,
                             Serializer<? super K> keySerializer,
                             Serializer<? super V> valueSerializer,
                             Iterable<KeyValue<? extends K, ? extends V>> records) {
        Inputs<? super K, ? super V> inputs = input(topic, keySerializer, valueSerializer);
        records.forEach(inputs::record);
        return this;
    }


    default <K, V> MockStreams input(String topic,
                             Serializer<? super K> keySerializer,
                             Serializer<? super V> valueSerializer,
                             KeyValue<? extends K, ? extends V>... records) {
        return input(topic, keySerializer, valueSerializer, Arrays.asList(records));
    }


    default <K, V> MockStreams input(String topic,
                             Serde<? super K> keySerde,
                             Serde<? super V> valueSerde,
                             Iterable<KeyValue<? extends K, ? extends V>> records) {
        return input(topic, keySerde.serializer(), valueSerde.serializer(), records);
    }


    default <K, V> MockStreams input(String topic,
                             Serde<? super K> keySerde,
                             Serde<? super V> valueSerde,
                             KeyValue<? extends K, ? extends V>... records) {
        return input(topic, keySerde.serializer(), valueSerde.serializer(), records);
    }


    <K, V> MockStreams verifyKeyValueStore(String name, Consumer<ReadOnlyKeyValueStore<K, V>> verifier);


    @SuppressWarnings("unchecked")
    <S extends StateStore> S stateStore(String name);


    interface Inputs<K, V> {
        
        Inputs<K, V> record(KeyValue<? extends K, ? extends V> keyValue);

        default Inputs<K, V> record(@Nullable K key, @Nullable V value) {
            return record(KeyValue.pair(key, value));
        }
    }
}
