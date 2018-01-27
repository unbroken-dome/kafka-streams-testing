package org.unbrokendome.kafkatesting.streams.junit5;

public class TopologyInitializerException extends RuntimeException {

    public TopologyInitializerException(String message) {
        super(message);
    }


    public TopologyInitializerException(String message, Throwable cause) {
        super(message, cause);
    }
}
