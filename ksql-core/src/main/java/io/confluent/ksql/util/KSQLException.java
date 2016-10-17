package io.confluent.ksql.util;


import org.apache.kafka.streams.errors.StreamsException;

public class KSQLException extends StreamsException {

    public KSQLException(String message) {
        super(message);
    }
}
