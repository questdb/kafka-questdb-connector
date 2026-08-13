package io.questdb.kafka.compat;

import io.questdb.client.std.NumericException;
import io.questdb.client.std.str.StringSink;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NumbersTest {

    @Test
    void testAppend() {
        StringSink sink = new StringSink();
        Numbers.append(sink, Integer.MIN_VALUE);
        sink.put(',');
        Numbers.append(sink, Long.MAX_VALUE);
        assertEquals("null,9223372036854775807", sink.toString());
    }

    @Test
    void testEncodeLowHighInts() {
        long encoded = Numbers.encodeLowHighInts(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertEquals(Integer.MIN_VALUE, Numbers.decodeLowInt(encoded));
        assertEquals(Integer.MAX_VALUE, Numbers.decodeHighInt(encoded));
    }

    @Test
    void testNotDigit() {
        assertFalse(Numbers.notDigit('0'));
        assertFalse(Numbers.notDigit('9'));
        assertTrue(Numbers.notDigit('/'));
        assertTrue(Numbers.notDigit(':'));
    }

    @Test
    void testParse000Greedy() throws NumericException {
        assertParsed(Numbers.parseInt000Greedy("2", 0, 1), 200, 1);
        assertParsed(Numbers.parseInt000Greedy("06", 0, 2), 60, 2);
        assertParsed(Numbers.parseInt000Greedy("219", 0, 3), 219, 3);
        assertParsed(Numbers.parseInt000Greedy("2x", 0, 2), 200, 1);
    }

    @Test
    void testParse000GreedyRejectsEmptyAndOverflow() {
        assertThrows(NumericException.class, () -> Numbers.parseInt000Greedy("", 0, 0));
        assertThrows(NumericException.class, () -> Numbers.parseInt000Greedy("1234", 0, 4));
    }

    @Test
    void testParseIntSafelyStopsAtDelimiter() throws NumericException {
        assertParsed(Numbers.parseIntSafely("1234x5", 0, 6), 1234, 4);
        assertParsed(Numbers.parseIntSafely("xx-42!", 2, 6), -42, 3);
        assertParsed(Numbers.parseIntSafely("12345", 0, 5), 12345, 5);
    }

    @Test
    void testParseIntSafelyAcceptsIntegerBoundaries() throws NumericException {
        assertParsed(Numbers.parseIntSafely("2147483647", 0, 10), Integer.MAX_VALUE, 10);
        assertParsed(Numbers.parseIntSafely("-2147483648", 0, 11), Integer.MIN_VALUE, 11);
    }

    @Test
    void testParseIntSafelyRejectsInvalidInputAndOverflow() {
        assertThrows(NumericException.class, () -> Numbers.parseIntSafely(null, 0, 0));
        assertThrows(NumericException.class, () -> Numbers.parseIntSafely("x", 0, 1));
        assertThrows(NumericException.class, () -> Numbers.parseIntSafely("-", 0, 1));
        assertThrows(NumericException.class, () -> Numbers.parseIntSafely("2147483648", 0, 10));
        assertThrows(NumericException.class, () -> Numbers.parseIntSafely("-2147483649", 0, 11));
    }

    @Test
    void testParseLong000000Greedy() throws NumericException {
        assertParsed(Numbers.parseLong000000Greedy("2", 0, 1), 200000, 1);
        assertParsed(Numbers.parseLong000000Greedy("000006", 0, 6), 6, 6);
        assertParsed(Numbers.parseLong000000Greedy("123456", 0, 6), 123456, 6);
        assertParsed(Numbers.parseLong000000Greedy("123x", 0, 4), 123000, 3);
    }

    @Test
    void testParseLong000000GreedyRejectsEmptyAndOverflow() {
        assertThrows(NumericException.class, () -> Numbers.parseLong000000Greedy("", 0, 0));
        assertThrows(NumericException.class, () -> Numbers.parseLong000000Greedy("1234567", 0, 7));
    }

    @Test
    void testParseIntSlice() throws NumericException {
        assertEquals(-42, Numbers.parseInt("xx-42yy", 2, 5));
    }

    private static void assertParsed(long parsed, int value, int length) {
        assertEquals(value, Numbers.decodeLowInt(parsed));
        assertEquals(length, Numbers.decodeHighInt(parsed));
    }
}
