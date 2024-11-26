package io.questdb.kafka;

import io.questdb.std.NumericException;
import org.apache.kafka.connect.errors.ConnectException;

public final class InvalidDataException extends ConnectException {
    public InvalidDataException(String message) {
        super(message);
    }

    public InvalidDataException(String message, NumericException e) {
        super(message, e);
    }
}
