package org.unbrokendome.kafkatesting.streams.junit5;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.unbrokendome.kafkatesting.streams.CommonTestTopology;
import org.unbrokendome.kafkatesting.streams.MockStreams;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.unbrokendome.kafkatesting.streams.KeyValueStoreMatchers.hasEntryFor;


@ExtendWith(MockStreamsExtension.class)
@SuppressWarnings("Duplicates")
public class MockStreamsExtensionTest {

    @Topology
    public void topology(StreamsBuilder builder) {
        new CommonTestTopology().accept(builder);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testTopicToList(MockStreams mockStreams) {
        mockStreams.input("plaintext-input", Serdes.String(), Serdes.String())
                .record(null, "hello world");

        List<KeyValue<String, Long>> entries =
                mockStreams.topic("wordcount-output", Serdes.String(), Serdes.Long()).toList();
        assertThat(entries, contains(
                new KeyValue<>("hello", 1L),
                new KeyValue<>("world", 1L)));
    }


    @Nested
    class WithMultipleInputs {

        @BeforeEach
        public void generateInputs(MockStreams mockStreams) {
            mockStreams.input("plaintext-input", Serdes.String(), Serdes.String())
                    .record(null, "all streams lead to kafka")
                    .record(null, "hello kafka streams");
        }


        @Test
        public void testTopicVerification(MockStreams mockStreams) {
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
        public void testTopicToMap(MockStreams mockStreams) {
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
        public void testStoreVerification(MockStreams mockStreams) {
            mockStreams.<String, Long>verifyKeyValueStore("Counts", (store) -> {
                assertThat(store.get("all"), equalTo(1L));
                assertThat(store.get("streams"), equalTo(2L));
                assertThat(store.get("lead"), equalTo(1L));
                assertThat(store.get("to"), equalTo(1L));
                assertThat(store.get("kafka"), equalTo(2L));
                assertThat(store.get("hello"), equalTo(1L));
            });
        }

        @Test
        public void testStoreVerificationWithMatcher(MockStreams mockStreams) {
            mockStreams.<String, Long>verifyKeyValueStore("Counts", (store) -> {
                assertThat(store, hasEntryFor("all", equalTo(1L)));
                assertThat(store, hasEntryFor("streams", equalTo(2L)));
                assertThat(store, hasEntryFor("lead", equalTo(1L)));
                assertThat(store, hasEntryFor("to", equalTo(1L)));
                assertThat(store, hasEntryFor("kafka", equalTo(2L)));
                assertThat(store, hasEntryFor("hello", equalTo(1L)));
            });
        }
    }
}
