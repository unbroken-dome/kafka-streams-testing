package org.unbrokendome.kafkatesting.streams.junit4;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.unbrokendome.kafkatesting.streams.CommonTestTopology;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;


@SuppressWarnings("Duplicates")
public class MockStreamsRuleTest {

    @Rule
    public final MockStreamsRule mockStreams = MockStreamsRule.builder()
            .withTopology(new CommonTestTopology())
            .build();

    @Test
    public void testTopicVerification() {

        mockStreams.input("plaintext-input", Serdes.String(), Serdes.String())
                .record(null, "all streams lead to kafka")
                .record(null, "hello kafka streams");

        mockStreams.topic("wordcount-output", Serdes.String(), Serdes.Long())
                .expectNext(equalTo("all"), equalTo(1L))
                .expectNext(equalTo("streams"), equalTo(1L))
                .expectNext(equalTo("lead"), equalTo(1L))
                .expectNext(equalTo("to"), equalTo(1L))
                .expectNext(equalTo("kafka"), equalTo(1L))
                .expectNext(equalTo("hello"), equalTo(1L))
                .expectNext(equalTo("kafka"), equalTo(2L))
                .expectNext(equalTo("streams"), equalTo(2L));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testTopicToList() {
        mockStreams.input("plaintext-input", Serdes.String(), Serdes.String())
                .record(null, "hello world");

        List<KeyValue<String, Long>> entries =
                mockStreams.topic("wordcount-output", Serdes.String(), Serdes.Long()).toList();
        assertThat(entries, contains(
                new KeyValue<>("hello", 1L),
                new KeyValue<>("world", 1L)));
    }


    @Test
    public void testTopicToMap() {
        mockStreams.input("plaintext-input", Serdes.String(), Serdes.String())
                .record(null, "all streams lead to kafka")
                .record(null, "hello kafka streams");

        Map<String, Long> table = mockStreams.topic("wordcount-output", Serdes.String(), Serdes.Long()).toMap();
        assertThat(table.entrySet(), Matchers.hasSize(6));
        assertThat(table, Matchers.hasEntry("all", 1L));
        assertThat(table, Matchers.hasEntry("streams", 2L));
        assertThat(table, Matchers.hasEntry("lead", 1L));
        assertThat(table, Matchers.hasEntry("to", 1L));
        assertThat(table, Matchers.hasEntry("kafka", 2L));
        assertThat(table, Matchers.hasEntry("hello", 1L));
    }


    @Test
    public void testStoreVerification() {

        mockStreams.input("plaintext-input", Serdes.String(), Serdes.String())
                .record(null, "all streams lead to kafka")
                .record(null, "hello kafka streams");

        mockStreams.<String, Long>verifyKeyValueStore("Counts", (store) -> {
            assertThat(store.get("all"), equalTo(1L));
            assertThat(store.get("streams"), equalTo(2L));
            assertThat(store.get("lead"), equalTo(1L));
            assertThat(store.get("to"), equalTo(1L));
            assertThat(store.get("kafka"), equalTo(2L));
            assertThat(store.get("hello"), equalTo(1L));
        });
    }
}
