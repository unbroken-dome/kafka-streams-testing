package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.ProcessorTopologyTestDriver;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Function;


public abstract class AbstractMockStreams implements MockStreams, AutoCloseable {

    @Nullable
    protected abstract ProcessorTopologyTestDriver getTestDriver();


    protected final <T> T withTestDriver(Function<ProcessorTopologyTestDriver, T> action) {
        ProcessorTopologyTestDriver testDriver = getTestDriver();
        if (testDriver == null) {
            throw new IllegalStateException("The ProcessorTopologyTestDriver has not been created - make sure " +
                    "to use the MockStreams instance only during the actual test");
        }
        return action.apply(testDriver);
    }


    @Override
    public final <K, V> TopicVerifier<K, V> topic(String topic, Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer) {
        return withTestDriver(testDriver ->
                new TopicVerifier<>(testDriver, topic, keyDeserializer, valueDeserializer));
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <K, V> TableVerifier<K, V> table(String stateStoreName) {
        return withTestDriver(testDriver -> {
            ReadOnlyKeyValueStore<K, V> stateStore = (ReadOnlyKeyValueStore<K, V>) testDriver.getStateStore(stateStoreName);
            return new TableVerifier<>(stateStore, stateStoreName);
        });
    }


    @Override
    public final MockStreams initStateStore(String name, Consumer<StateStore> initializer) {
        return withTestDriver(testDriver -> {
            StateStore stateStore = testDriver.getStateStore(name);
            initializer.accept(stateStore);
            return this;
        });
    }


    @Override
    public final <K, V> MockStreams initKeyValueStore(String name, Consumer<KeyValueStore<K, V>> initializer) {
        return withTestDriver(testDriver -> {
            KeyValueStore<K, V> keyValueStore = testDriver.getKeyValueStore(name);
            initializer.accept(keyValueStore);
            return this;
        });
    }


    @Override
    public final <K, V> Inputs<K, V> input(String topic, Serializer<? super K> keySerializer,
                                     Serializer<? super V> valueSerializer) {
        return withTestDriver(testDriver -> new Inputs<K, V>() {
            @Override
            public Inputs<K, V> record(KeyValue<? extends K, ? extends V> keyValue) {
                testDriver.process(topic, keyValue.key, keyValue.value, keySerializer, valueSerializer);
                return this;
            }
        });
    }


    @Override
    public final <K, V> MockStreams verifyKeyValueStore(String name, Consumer<ReadOnlyKeyValueStore<K, V>> verifier) {
        return withTestDriver(testDriver -> {
            ReadOnlyKeyValueStore<K, V> keyValueStore = testDriver.getKeyValueStore(name);
            verifier.accept(keyValueStore);
            return this;
        });
    }


    @Override
    @SuppressWarnings("unchecked")
    public final <S extends StateStore> S stateStore(String name) {
        return withTestDriver(testDriver -> (S) testDriver.getStateStore(name));
    }


    @Override
    public void close() {
        ProcessorTopologyTestDriver testDriver = getTestDriver();
        if (testDriver != null) {
            testDriver.close();
        }
    }
}
