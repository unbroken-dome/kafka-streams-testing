package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.hamcrest.Matcher;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.unbrokendome.kafkatesting.streams.KeyValueStoreMatchers.hasEntryFor;
import static org.unbrokendome.kafkatesting.streams.KeyValueStoreMatchers.hasNoEntryFor;
import static org.unbrokendome.kafkatesting.streams.KeyValueStoreMatchers.isEmpty;


public class TableVerifier<K, V> {

    private final ReadOnlyKeyValueStore<K, V> stateStore;
    private final String stateStoreName;

    public TableVerifier(ReadOnlyKeyValueStore<K, V> stateStore, String stateStoreName) {
        this.stateStore = stateStore;
        this.stateStoreName = stateStoreName;
    }

    public TableVerifier<K, V> expectEntry(K key, V value) {
        assertThat("State store \"" + stateStoreName + "\"", stateStore, hasEntryFor(key, value));
        return this;
    }

    public TableVerifier<K, V> expectEntry(K key, Matcher<? super V> valueMatcher) {
        assertThat("State store \"" + stateStoreName + "\"", stateStore, hasEntryFor(key, valueMatcher));
        return this;
    }

    public TableVerifier<K, V> expectNoEntry(K key) {
        assertThat("State store \"" + stateStoreName + "\"", stateStore, hasNoEntryFor(key));
        return this;
    }

    public TableVerifier<K, V> expectEmpty() {
        assertThat("State store \"" + stateStoreName + "\"", stateStore, isEmpty());
        return this;
    }
}
