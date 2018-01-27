package org.unbrokendome.kafkatesting.streams.junit4;

import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.unbrokendome.kafkatesting.streams.AbstractMockStreams;
import org.unbrokendome.kafkatesting.streams.MockStreamsBuilder;

import javax.annotation.Nullable;
import java.util.function.Supplier;


/**
 * JUnit4 test rule that sets up a {@link ProcessorTopologyTestDriver}.
 */
public class MockStreamsRule extends AbstractMockStreams implements TestRule {

    private final Supplier<ProcessorTopologyTestDriver> testDriverSupplier;
    @Nullable
    private ProcessorTopologyTestDriver testDriver;


    public MockStreamsRule(Supplier<ProcessorTopologyTestDriver> testDriverSupplier) {
        this.testDriverSupplier = testDriverSupplier;
    }


    @Nullable
    @Override
    protected ProcessorTopologyTestDriver getTestDriver() {
        return testDriver;
    }


    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                testDriver = testDriverSupplier.get();
                try {
                    base.evaluate();
                } finally {
                    assert testDriver != null;
                    testDriver.close();
                }
            }
        };
    }


    public static Builder builder() {
        return new Builder();
    }


    public static class Builder extends MockStreamsBuilder<MockStreamsRule, Builder> {

        @Override
        public MockStreamsRule build() {
            return new MockStreamsRule(this::buildTestDriver);
        }
    }
}
