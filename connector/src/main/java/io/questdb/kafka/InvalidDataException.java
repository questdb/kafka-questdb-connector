package io.questdb.kafka;

import org.apache.kafka.connect.errors.ConnectException;

public final class InvalidDataException extends ConnectException {
    public InvalidDataException(String message) {
        super(message);
    }

    public InvalidDataException(String message, Throwable e) {
        super(message, e);
    }
}
