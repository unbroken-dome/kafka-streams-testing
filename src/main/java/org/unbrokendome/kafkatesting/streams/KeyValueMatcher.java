package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.streams.KeyValue;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import javax.annotation.Nullable;


public class KeyValueMatcher<K, V> extends TypeSafeDiagnosingMatcher<KeyValue<K, V>> {

    private final Matcher<? super K> keyMatcher;
    private final Matcher<? super V> valueMatcher;


    public KeyValueMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
        this.keyMatcher = keyMatcher;
        this.valueMatcher = valueMatcher;
    }


    @Override
    protected boolean matchesSafely(@Nullable KeyValue<K, V> item, Description mismatchDescription) {
        if (item == null) {
            mismatchDescription.appendText("topic had no more records");
            return false;
        }

        K key = item.key;
        if (!keyMatcher.matches(key)) {
            mismatchDescription.appendText("key ");
            keyMatcher.describeMismatch(key, mismatchDescription);
            return false;
        }

        V value = item.value;
        if (!valueMatcher.matches(value)) {
            mismatchDescription.appendText("value ");
            valueMatcher.describeMismatch(value, mismatchDescription);
            return false;
        }

        return true;
    }


    @Override
    public void describeTo(Description description) {
        description.appendText("KeyValue with key (")
                .appendDescriptionOf(keyMatcher)
                .appendText(") and value (")
                .appendDescriptionOf(valueMatcher)
                .appendText(")");
    }
}
