package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nullable;


/**
 * Hamcrest matchers for testing the contents of Kafka Streams' {@linkplain ReadOnlyKeyValueStore key-value stores}.
 */
public final class KeyValueStoreMatchers {

    public static <K, V> Matcher<ReadOnlyKeyValueStore<K, V>> hasEntryFor(K key) {
        return new HasEntry<>(key, true, null);
    }


    public static <K, V> Matcher<ReadOnlyKeyValueStore<K, V>> hasEntryFor(K key, Matcher<? super V> valueMatcher) {
        return new HasEntry<>(key, true, valueMatcher);
    }


    public static <K, V> Matcher<ReadOnlyKeyValueStore<K, V>> hasEntryFor(K key, V value) {
        return hasEntryFor(key, CoreMatchers.equalTo(value));
    }


    public static <K, V> Matcher<ReadOnlyKeyValueStore<K, V>> hasNoEntryFor(K key) {
        return new HasEntry<>(key, false, null);
    }


    public static Matcher<ReadOnlyKeyValueStore<?, ?>> isEmpty() {
        return new IsEmpty();
    }


    private static class HasEntry<K, V> extends TypeSafeDiagnosingMatcher<ReadOnlyKeyValueStore<K, V>> {

        private final K key;
        private boolean expectValue;

        @Nullable
        private final Matcher<? super V> valueMatcher;


        public HasEntry(K key, boolean expectValue, @Nullable Matcher<? super V> valueMatcher) {
            this.key = key;
            this.expectValue = expectValue;
            this.valueMatcher = expectValue ? valueMatcher : null;
        }


        @Override
        protected boolean matchesSafely(ReadOnlyKeyValueStore<K, V> store, Description mismatchDescription) {
            V value = store.get(key);

            if (value == null && expectValue) {
                mismatchDescription.appendText("no such entry in store");
                return false;
            }

            if (value != null && !expectValue) {
                mismatchDescription.appendText("entry was present with value: ")
                        .appendValue(value);
                return false;
            }

            if (valueMatcher != null && !valueMatcher.matches(value)) {
                mismatchDescription.appendText("value ");
                valueMatcher.describeMismatch(value, mismatchDescription);
                return false;
            }

            return true;
        }


        @Override
        public void describeTo(Description description) {
            description.appendText("Store has ")
                    .appendText(expectValue ? "entry" : "no entry")
                    .appendText(" for ").appendValue(key);
            if (valueMatcher != null) {
                description.appendText(" with value ").appendDescriptionOf(valueMatcher);
            } else {
                description.appendText(" with any value");
            }
        }
    }


    private static class IsEmpty extends TypeSafeDiagnosingMatcher<ReadOnlyKeyValueStore<?, ?>> {

        @Override
        protected boolean matchesSafely(ReadOnlyKeyValueStore<?, ?> store, Description mismatchDescription) {
            try (KeyValueIterator<?, ?> iterator = store.all()) {
                if (iterator.hasNext()) {
                    mismatchDescription.appendText("had ")
                            .appendValue(store.approximateNumEntries())
                            .appendText(" entries");
                    return false;
                }
            }
            return true;
        }


        @Override
        public void describeTo(Description description) {
            description.appendText("is empty");
        }
    }
}
