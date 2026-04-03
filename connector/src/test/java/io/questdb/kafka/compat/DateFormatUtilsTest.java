package io.questdb.kafka.compat;

import io.questdb.client.std.NumericException;
import io.questdb.client.std.str.StringSink;
import io.questdb.kafka.compat.datetime.millitime.DateFormatUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link DateFormatUtils}.
 */
public class DateFormatUtilsTest {

    private StringSink sink;

    @BeforeEach
    void setUp() {
        sink = new StringSink();
    }

    // -------------------------------------------------------------------------
    // append0 -- zero-pads to at least 2 digits
    // -------------------------------------------------------------------------

    @Test
    void append0_singleDigit() {
        DateFormatUtils.append0(sink, 5);
        assertEquals("05", sink.toString());
    }

    @Test
    void append0_zero() {
        DateFormatUtils.append0(sink, 0);
        assertEquals("00", sink.toString());
    }

    @Test
    void append0_nine() {
        DateFormatUtils.append0(sink, 9);
        assertEquals("09", sink.toString());
    }

    @Test
    void append0_ten_noPadding() {
        DateFormatUtils.append0(sink, 10);
        assertEquals("10", sink.toString());
    }

    @Test
    void append0_twoDigit() {
        DateFormatUtils.append0(sink, 42);
        assertEquals("42", sink.toString());
    }

    @Test
    void append0_threeDigit() {
        DateFormatUtils.append0(sink, 123);
        assertEquals("123", sink.toString());
    }

    @Test
    void append0_negativeSingleDigit() {
        DateFormatUtils.append0(sink, -5);
        assertEquals("0-5", sink.toString());
    }

    @Test
    void append0_negativeTwoDigit() {
        DateFormatUtils.append0(sink, -12);
        assertEquals("-12", sink.toString());
    }

    // -------------------------------------------------------------------------
    // append00 -- zero-pads to at least 3 digits
    // -------------------------------------------------------------------------

    @Test
    void append00_singleDigit() {
        DateFormatUtils.append00(sink, 5);
        assertEquals("005", sink.toString());
    }

    @Test
    void append00_zero() {
        DateFormatUtils.append00(sink, 0);
        assertEquals("000", sink.toString());
    }

    @Test
    void append00_nine() {
        DateFormatUtils.append00(sink, 9);
        assertEquals("009", sink.toString());
    }

    @Test
    void append00_twoDigit() {
        DateFormatUtils.append00(sink, 42);
        assertEquals("042", sink.toString());
    }

    @Test
    void append00_99() {
        DateFormatUtils.append00(sink, 99);
        assertEquals("099", sink.toString());
    }

    @Test
    void append00_100_noPadding() {
        DateFormatUtils.append00(sink, 100);
        assertEquals("100", sink.toString());
    }

    @Test
    void append00_threeDigit() {
        DateFormatUtils.append00(sink, 456);
        assertEquals("456", sink.toString());
    }

    @Test
    void append00_999() {
        DateFormatUtils.append00(sink, 999);
        assertEquals("999", sink.toString());
    }

    @Test
    void append00_fourDigit() {
        DateFormatUtils.append00(sink, 1234);
        assertEquals("1234", sink.toString());
    }

    @Test
    void append00_negativeSingleDigit() {
        DateFormatUtils.append00(sink, -3);
        assertEquals("00-3", sink.toString());
    }

    @Test
    void append00_negativeTwoDigit() {
        DateFormatUtils.append00(sink, -42);
        assertEquals("0-42", sink.toString());
    }

    @Test
    void append00_negativeThreeDigit() {
        DateFormatUtils.append00(sink, -123);
        assertEquals("-123", sink.toString());
    }

    // -------------------------------------------------------------------------
    // appendHour121 -- 12-hour clock (1-12)
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
            "0,  12",  // midnight -> 12
            "1,  1",
            "2,  2",
            "11, 11",
            "12, 12",  // noon -> 12
            "13, 1",
            "14, 2",
            "23, 11",
    })
    void appendHour121(int hour24, int expected12) {
        DateFormatUtils.appendHour121(sink, hour24);
        assertEquals(String.valueOf(expected12), sink.toString());
    }

    @Test
    void appendHour121_fullCycle() {
        // Verify all 24 hours map to 1-12 range
        for (int h = 0; h < 24; h++) {
            sink.clear();
            DateFormatUtils.appendHour121(sink, h);
            int result = Integer.parseInt(sink.toString());
            assertTrue(result >= 1 && result <= 12,
                    "Hour " + h + " mapped to " + result + ", expected 1-12");
        }
    }

    // -------------------------------------------------------------------------
    // appendHour241 -- 24-hour clock (1-24)
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
            "0,  24",  // midnight -> 24
            "1,  1",
            "2,  2",
            "12, 12",
            "13, 13",
            "23, 23",
    })
    void appendHour241(int hour24, int expected241) {
        DateFormatUtils.appendHour241(sink, hour24);
        assertEquals(String.valueOf(expected241), sink.toString());
    }

    @Test
    void appendHour241_fullCycle() {
        // Verify all 24 hours map to 1-24 range
        for (int h = 0; h < 24; h++) {
            sink.clear();
            DateFormatUtils.appendHour241(sink, h);
            int result = Integer.parseInt(sink.toString());
            assertTrue(result >= 1 && result <= 24,
                    "Hour " + h + " mapped to " + result + ", expected 1-24");
        }
    }

    // -------------------------------------------------------------------------
    // assertRemaining -- boundary checks
    // -------------------------------------------------------------------------

    @Test
    void assertRemaining_posLessThanHi_noException() {
        assertDoesNotThrow(() -> DateFormatUtils.assertRemaining(0, 5));
    }

    @Test
    void assertRemaining_posOneLessThanHi_noException() {
        assertDoesNotThrow(() -> DateFormatUtils.assertRemaining(4, 5));
    }

    @Test
    void assertRemaining_posEqualsHi_throwsException() {
        assertThrows(NumericException.class, () -> DateFormatUtils.assertRemaining(5, 5));
    }

    @Test
    void assertRemaining_posGreaterThanHi_throwsException() {
        assertThrows(NumericException.class, () -> DateFormatUtils.assertRemaining(6, 5));
    }

    @Test
    void assertRemaining_zeroZero_throwsException() {
        assertThrows(NumericException.class, () -> DateFormatUtils.assertRemaining(0, 0));
    }

    @Test
    void assertRemaining_negativePos_noException() {
        assertDoesNotThrow(() -> DateFormatUtils.assertRemaining(-1, 0));
    }

    // -------------------------------------------------------------------------
    // assertString -- delimiter matching
    // -------------------------------------------------------------------------

    @Test
    void assertString_simpleDelimiter_matches() throws NumericException {
        // delimiter "T", input has "T" at position 10
        String delimiter = "T";
        String input = "2024-03-15T10:30:00";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 10, input.length());
        assertEquals(11, newPos);
    }

    @Test
    void assertString_simpleDelimiter_doesNotMatch() {
        String delimiter = "T";
        String input = "2024-03-15X10:30:00";
        assertThrows(NumericException.class,
                () -> DateFormatUtils.assertString(delimiter, delimiter.length(), input, 10, input.length()));
    }

    @Test
    void assertString_multiCharDelimiter() throws NumericException {
        String delimiter = "UTC";
        String input = "2024-03-15UTC";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 10, input.length());
        assertEquals(13, newPos);
    }

    @Test
    void assertString_quotedDelimiter_matches() throws NumericException {
        // Quoted delimiter: the quotes are stripped and inner text is matched
        String delimiter = "'at'";
        String input = "xxxatyyy";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 3, input.length());
        assertEquals(5, newPos);
    }

    @Test
    void assertString_quotedDelimiter_doesNotMatch() {
        String delimiter = "'at'";
        String input = "xxxxyyyy";
        assertThrows(NumericException.class,
                () -> DateFormatUtils.assertString(delimiter, delimiter.length(), input, 3, input.length()));
    }

    @Test
    void assertString_quotedSingleChar() throws NumericException {
        // Quoted with single inner char: 'T'
        String delimiter = "'T'";
        String input = "xTy";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 1, input.length());
        assertEquals(2, newPos);
    }

    @Test
    void assertString_insufficientRemaining_throws() {
        String delimiter = "LONG";
        String input = "ab";
        assertThrows(NumericException.class,
                () -> DateFormatUtils.assertString(delimiter, delimiter.length(), input, 0, input.length()));
    }

    @Test
    void assertString_atEndBoundary() throws NumericException {
        // Delimiter exactly consumes remaining input
        String delimiter = "Z";
        String input = "2024Z";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 4, input.length());
        assertEquals(5, newPos);
    }

    @Test
    void assertString_quotedMultiChar() throws NumericException {
        String delimiter = "'hello'";
        String input = "XXXhelloYYY";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 3, input.length());
        assertEquals(8, newPos);
    }

    @Test
    void assertString_quotedMultiChar_doesNotMatch() {
        String delimiter = "'hello'";
        String input = "XXXworldYYY";
        assertThrows(NumericException.class,
                () -> DateFormatUtils.assertString(delimiter, delimiter.length(), input, 3, input.length()));
    }

    @Test
    void assertString_posAtStart() throws NumericException {
        String delimiter = "AB";
        String input = "ABCD";
        int newPos = DateFormatUtils.assertString(delimiter, delimiter.length(), input, 0, input.length());
        assertEquals(2, newPos);
    }

    // -------------------------------------------------------------------------
    // append0 / append00 cumulative (multiple appends to same sink)
    // -------------------------------------------------------------------------

    @Test
    void append0_cumulativeAppends() {
        DateFormatUtils.append0(sink, 1);
        sink.putAscii(':');
        DateFormatUtils.append0(sink, 30);
        sink.putAscii(':');
        DateFormatUtils.append0(sink, 5);
        assertEquals("01:30:05", sink.toString());
    }

    @Test
    void append00_cumulativeAppends() {
        DateFormatUtils.append00(sink, 1);
        sink.putAscii('.');
        DateFormatUtils.append00(sink, 42);
        sink.putAscii('.');
        DateFormatUtils.append00(sink, 999);
        assertEquals("001.042.999", sink.toString());
    }
}
