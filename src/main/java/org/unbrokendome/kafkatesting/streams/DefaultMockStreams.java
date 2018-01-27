package org.unbrokendome.kafkatesting.streams;

import org.apache.kafka.test.ProcessorTopologyTestDriver;

import javax.annotation.Nonnull;


@SuppressWarnings("UnusedReturnValue")
public class DefaultMockStreams extends AbstractMockStreams {

    private final ProcessorTopologyTestDriver testDriver;


    public DefaultMockStreams(ProcessorTopologyTestDriver testDriver) {
        this.testDriver = testDriver;
    }


    @Nonnull
    @Override
    public ProcessorTopologyTestDriver getTestDriver() {
        return testDriver;
    }


    public static Builder builder() {
        return new Builder();
    }


    public static class Builder extends MockStreamsBuilder<DefaultMockStreams, Builder> {

        @Override
        public DefaultMockStreams build() {
            return new DefaultMockStreams(buildTestDriver());
        }
    }
}
