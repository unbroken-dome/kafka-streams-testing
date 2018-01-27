package org.unbrokendome.kafkatesting.streams.junit5;

import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ReflectionUtils;
import org.unbrokendome.kafkatesting.streams.DefaultMockStreams;
import org.unbrokendome.kafkatesting.streams.MockStreams;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;


public class MockStreamsExtension implements BeforeAllCallback, AfterEachCallback, ParameterResolver {

    @FunctionalInterface
    private interface TopologyInitializer extends BiConsumer<Object, StreamsBuilder> {

        default TopologyInitializer andThen(TopologyInitializer after) {
            return ((testInstance, streamsBuilder) -> {
                this.accept(testInstance, streamsBuilder);
                after.accept(testInstance, streamsBuilder);
            });
        }


        default TopologyInitializer withParent(TopologyInitializer parentInitializer) {
            return ((testInstance, streamsBuilder) -> {

                Object outerTestInstance;
                try {
                    Field outerField = testInstance.getClass().getDeclaredField("this$0");
                    outerField.setAccessible(true);
                    outerTestInstance = outerField.get(testInstance);

                } catch (ReflectiveOperationException ex) {
                    throw new IllegalStateException(ex);
                }

                parentInitializer.accept(outerTestInstance, streamsBuilder);
                this.accept(testInstance, streamsBuilder);
            });
        }
    }


    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(MockStreamsExtension.class);
    private static final String TOPOLOGY_INITIALIZER_KEY = "topologyInitializer";
    private static final String MOCK_STREAMS_KEY = "mockStreams";

    private static final TopologyInitializer EMPTY_INITIALIZER = (c, b) -> { };



    @Override
    public void beforeAll(ExtensionContext context) {

        Class<?> testClass = context.getRequiredTestClass();

        TopologyInitializer topologyInitializer = EMPTY_INITIALIZER;

        List<Method> topologyMethods = ReflectionUtils.findMethods(testClass,
                method -> method.isAnnotationPresent(Topology.class));
        for (Method topologyMethod : topologyMethods) {
            if (topologyMethod.getParameterCount() == 1 &&
                    topologyMethod.getParameterTypes()[0].isAssignableFrom(StreamsBuilder.class)) {

                topologyInitializer = topologyInitializer.andThen((testInstance, builder) -> {
                    try {
                        topologyMethod.invoke(testInstance, builder);
                    } catch (ReflectiveOperationException ex) {
                        throw new TopologyInitializerException("Failed to initialize topology", ex);
                    }
                });

            } else {
                throw new TopologyInitializerException("Invalid @Topology method " + topologyMethod +
                        " - must accept a single parameter of type StreamsBuilder");
            }
        }

        for (Optional<ExtensionContext> parentContextOpt = context.getParent();
             parentContextOpt.isPresent();
             parentContextOpt = parentContextOpt.flatMap(ExtensionContext::getParent)) {

            ExtensionContext parentContext = parentContextOpt.get();

            TopologyInitializer parentTopologyInitializer =
                    parentContext.getStore(NAMESPACE).get(TOPOLOGY_INITIALIZER_KEY, TopologyInitializer.class);

            if (parentTopologyInitializer != null) {
                topologyInitializer = topologyInitializer.withParent(parentTopologyInitializer);
            }
        }


        context.getStore(NAMESPACE).put(TOPOLOGY_INITIALIZER_KEY, topologyInitializer);
    }


    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getTarget().isPresent() &&
                parameterContext.getParameter().getType() == MockStreams.class;
    }


    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {

        ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);

        return store.getOrComputeIfAbsent(MOCK_STREAMS_KEY, key -> {
            TopologyInitializer topologyInitializer = store.get(TOPOLOGY_INITIALIZER_KEY, TopologyInitializer.class);
            return DefaultMockStreams.builder()
                    .withTopology((streamBuilder) ->
                            topologyInitializer.accept(extensionContext.getRequiredTestInstance(), streamBuilder))
                    .build();
        });
    }


    @Override
    public void afterEach(ExtensionContext context) throws Exception {

        ExtensionContext.Store store = context.getStore(NAMESPACE);

        MockStreams mockStreams = store.get(MOCK_STREAMS_KEY, MockStreams.class);
        if (mockStreams instanceof AutoCloseable) {
            ((AutoCloseable) mockStreams).close();
        }

        store.remove(MOCK_STREAMS_KEY);
    }
}
