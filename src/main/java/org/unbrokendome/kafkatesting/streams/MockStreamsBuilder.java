package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.ProcessorTopologyTestDriver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public abstract class MockStreamsBuilder<T extends MockStreams, B extends MockStreamsBuilder<T, B>> {

    protected final Map<String, Object> configMap = new HashMap<>();
    protected Consumer<StreamsBuilder> topology = (b) -> { };
    protected Consumer<ProcessorTopologyTestDriver> initializer = (d) -> { };


    public MockStreamsBuilder() {
        configMap.put(StreamsConfig.APPLICATION_ID_CONFIG, "mockstreams-" + UUID.randomUUID());
        configMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }


    @SuppressWarnings("unchecked")
    protected final B self() {
        return (B) this;
    }
    

    public B withConfig(String key, Object value) {
        configMap.put(key, value);
        return self();
    }


    public B withConfig(Map<String, ?> config) {
        configMap.putAll(config);
        return self();
    }


    public B withTopology(Consumer<StreamsBuilder> topology) {
        this.topology = this.topology.andThen(topology);
        return self();
    }


    public B withStateStore(String name, Consumer<? super StateStore> initializer) {
        return withInitializer(driver -> {
            StateStore stateStore = driver.getStateStore(name);
            initializer.accept(stateStore);
        });
    }


    public <K, V> B withKeyValueStore(String name, Consumer<? super KeyValueStore<K, V>> initializer) {
        return withInitializer(driver -> {
            KeyValueStore<K, V> keyValueStore = driver.getKeyValueStore(name);
            initializer.accept(keyValueStore);
        });
    }


    public <K, V> B withKeyValueStore(String name, List<KeyValue<K, V>> entries) {
        return this.<K, V>withKeyValueStore(name, kvs -> kvs.putAll(entries));
    }


    public <K, V> B withKeyValueStore(String name, Map<K, V> entries) {
        return withKeyValueStore(name,
                entries.entrySet().stream()
                        .map(entry -> new KeyValue<>(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList()));
    }


    private B withInitializer(Consumer<ProcessorTopologyTestDriver> initializer) {
        this.initializer = this.initializer.andThen(initializer);
        return self();
    }


    protected final ProcessorTopologyTestDriver buildTestDriver() {
        StreamsBuilder streamBuilder = new StreamsBuilder();
        topology.accept(streamBuilder);
        ProcessorTopologyTestDriver testDriver = new ProcessorTopologyTestDriver(
                new StreamsConfig(configMap), streamBuilder.build());
        initializer.accept(testDriver);
        return testDriver;
    }


    public abstract T build();
}
