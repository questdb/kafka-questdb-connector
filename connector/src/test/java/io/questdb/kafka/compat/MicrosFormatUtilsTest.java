package io.questdb.kafka.compat;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.str.StringSink;
import io.questdb.kafka.compat.datetime.CommonUtils;
import io.questdb.kafka.compat.datetime.DateLocale;
import io.questdb.kafka.compat.datetime.DateLocaleFactory;
import io.questdb.kafka.compat.datetime.microtime.Micros;
import io.questdb.kafka.compat.datetime.microtime.MicrosFormatUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link MicrosFormatUtils}.
 */
public class MicrosFormatUtilsTest {
    private static final DateLocale EN = DateLocaleFactory.EN_LOCALE;
    private StringSink sink;

    @BeforeEach
    void setUp() {
        sink = new StringSink();
    }

    // -----------------------------------------------------------------------
    // adjustYear
    // -----------------------------------------------------------------------
    @Nested
    class AdjustYear {
        @Test
        void adjustYearZero() {
            int result = MicrosFormatUtils.adjustYear(0);
            // thisCenturyLow is derived from current year; for 2026, centuryOffset=26,
            // thisCenturyLimit=46 (<=100), so thisCenturyLow = 2000.
            // adjustYear(0) = 2000
            assertTrue(result >= 1900 && result <= 2200,
                    "adjusted year for 0 should be a reasonable century base, got " + result);
        }

        @Test
        void adjustYearSmallValue() {
            int base = MicrosFormatUtils.adjustYear(0);
            assertEquals(base + 5, MicrosFormatUtils.adjustYear(5));
        }

        @Test
        void adjustYearMaxTwoDigit() {
            int base = MicrosFormatUtils.adjustYear(0);
            assertEquals(base + 99, MicrosFormatUtils.adjustYear(99));
        }

        @Test
        void adjustYearIsConsistent() {
            // adjustYear should just add offset to the input
            int base = MicrosFormatUtils.adjustYear(0);
            for (int i = 0; i < 100; i++) {
                assertEquals(base + i, MicrosFormatUtils.adjustYear(i));
            }
        }
    }

    // -----------------------------------------------------------------------
    // append0 -- pads single-digit values with one leading zero
    // -----------------------------------------------------------------------
    @Nested
    class Append0 {
        @Test
        void singleDigit() {
            MicrosFormatUtils.append0(sink, 5);
            assertEquals("05", sink.toString());
        }

        @Test
        void zero() {
            MicrosFormatUtils.append0(sink, 0);
            assertEquals("00", sink.toString());
        }

        @Test
        void nine() {
            MicrosFormatUtils.append0(sink, 9);
            assertEquals("09", sink.toString());
        }

        @Test
        void twoDigits() {
            MicrosFormatUtils.append0(sink, 10);
            assertEquals("10", sink.toString());
        }

        @Test
        void twoDigitsLarge() {
            MicrosFormatUtils.append0(sink, 59);
            assertEquals("59", sink.toString());
        }

        @Test
        void threeDigitsNoPadding() {
            MicrosFormatUtils.append0(sink, 123);
            assertEquals("123", sink.toString());
        }

        @Test
        void negativeSmall() {
            MicrosFormatUtils.append0(sink, -5);
            assertEquals("0-5", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // append00 -- pads to at least 3 digits (2 leading zeros for single digit)
    // -----------------------------------------------------------------------
    @Nested
    class Append00 {
        @Test
        void singleDigit() {
            MicrosFormatUtils.append00(sink, 5);
            assertEquals("005", sink.toString());
        }

        @Test
        void zero() {
            MicrosFormatUtils.append00(sink, 0);
            assertEquals("000", sink.toString());
        }

        @Test
        void twoDigits() {
            MicrosFormatUtils.append00(sink, 42);
            assertEquals("042", sink.toString());
        }

        @Test
        void threeDigits() {
            MicrosFormatUtils.append00(sink, 123);
            assertEquals("123", sink.toString());
        }

        @Test
        void fourDigitsNoPadding() {
            MicrosFormatUtils.append00(sink, 1234);
            assertEquals("1234", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // append00000 -- pads to at least 6 digits (5 leading zeros for single digit)
    // -----------------------------------------------------------------------
    @Nested
    class Append00000 {
        @Test
        void zero() {
            MicrosFormatUtils.append00000(sink, 0);
            assertEquals("000000", sink.toString());
        }

        @Test
        void singleDigit() {
            MicrosFormatUtils.append00000(sink, 7);
            assertEquals("000007", sink.toString());
        }

        @Test
        void twoDigits() {
            MicrosFormatUtils.append00000(sink, 42);
            assertEquals("000042", sink.toString());
        }

        @Test
        void threeDigits() {
            MicrosFormatUtils.append00000(sink, 456);
            assertEquals("000456", sink.toString());
        }

        @Test
        void fourDigits() {
            MicrosFormatUtils.append00000(sink, 1234);
            assertEquals("001234", sink.toString());
        }

        @Test
        void fiveDigits() {
            MicrosFormatUtils.append00000(sink, 12345);
            assertEquals("012345", sink.toString());
        }

        @Test
        void sixDigits() {
            MicrosFormatUtils.append00000(sink, 123456);
            assertEquals("123456", sink.toString());
        }

        @Test
        void sevenDigitsNoPadding() {
            MicrosFormatUtils.append00000(sink, 1234567);
            assertEquals("1234567", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendYear -- appends year, treating 0 as 1
    // -----------------------------------------------------------------------
    @Nested
    class AppendYear {
        @Test
        void positiveYear() {
            MicrosFormatUtils.appendYear(sink, 2024);
            assertEquals("2024", sink.toString());
        }

        @Test
        void yearZeroBecomesOne() {
            MicrosFormatUtils.appendYear(sink, 0);
            assertEquals("1", sink.toString());
        }

        @Test
        void negativeYear() {
            MicrosFormatUtils.appendYear(sink, -500);
            assertEquals("-500", sink.toString());
        }

        @Test
        void yearOne() {
            MicrosFormatUtils.appendYear(sink, 1);
            assertEquals("1", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendYear0 -- pads to at least 2 digits
    // -----------------------------------------------------------------------
    @Nested
    class AppendYear0 {
        @Test
        void singleDigitYear() {
            MicrosFormatUtils.appendYear0(sink, 5);
            assertEquals("05", sink.toString());
        }

        @Test
        void twoDigitYear() {
            MicrosFormatUtils.appendYear0(sink, 24);
            assertEquals("24", sink.toString());
        }

        @Test
        void fourDigitYear() {
            MicrosFormatUtils.appendYear0(sink, 2024);
            assertEquals("2024", sink.toString());
        }

        @Test
        void yearZeroBecomesOne() {
            MicrosFormatUtils.appendYear0(sink, 0);
            assertEquals("01", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendYear00 -- pads to at least 3 digits
    // -----------------------------------------------------------------------
    @Nested
    class AppendYear00 {
        @Test
        void singleDigitYear() {
            MicrosFormatUtils.appendYear00(sink, 5);
            assertEquals("005", sink.toString());
        }

        @Test
        void twoDigitYear() {
            MicrosFormatUtils.appendYear00(sink, 24);
            assertEquals("024", sink.toString());
        }

        @Test
        void threeDigitYear() {
            MicrosFormatUtils.appendYear00(sink, 324);
            assertEquals("324", sink.toString());
        }

        @Test
        void fourDigitYear() {
            MicrosFormatUtils.appendYear00(sink, 2024);
            assertEquals("2024", sink.toString());
        }

        @Test
        void yearZero() {
            MicrosFormatUtils.appendYear00(sink, 0);
            assertEquals("001", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendYear000 -- pads to at least 4 digits
    // -----------------------------------------------------------------------
    @Nested
    class AppendYear000 {
        @Test
        void singleDigitYear() {
            MicrosFormatUtils.appendYear000(sink, 5);
            assertEquals("0005", sink.toString());
        }

        @Test
        void twoDigitYear() {
            MicrosFormatUtils.appendYear000(sink, 24);
            assertEquals("0024", sink.toString());
        }

        @Test
        void threeDigitYear() {
            MicrosFormatUtils.appendYear000(sink, 324);
            assertEquals("0324", sink.toString());
        }

        @Test
        void fourDigitYear() {
            MicrosFormatUtils.appendYear000(sink, 2024);
            assertEquals("2024", sink.toString());
        }

        @Test
        void fiveDigitYear() {
            MicrosFormatUtils.appendYear000(sink, 12024);
            assertEquals("12024", sink.toString());
        }

        @Test
        void yearZero() {
            MicrosFormatUtils.appendYear000(sink, 0);
            assertEquals("0001", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendAmPm
    // -----------------------------------------------------------------------
    @Nested
    class AppendAmPm {
        @Test
        void midnight() {
            MicrosFormatUtils.appendAmPm(sink, 0, EN);
            assertEquals("AM", sink.toString());
        }

        @Test
        void earlyMorning() {
            MicrosFormatUtils.appendAmPm(sink, 6, EN);
            assertEquals("AM", sink.toString());
        }

        @Test
        void elevenAm() {
            MicrosFormatUtils.appendAmPm(sink, 11, EN);
            assertEquals("AM", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendAmPm(sink, 12, EN);
            assertEquals("PM", sink.toString());
        }

        @Test
        void afternoon() {
            MicrosFormatUtils.appendAmPm(sink, 15, EN);
            assertEquals("PM", sink.toString());
        }

        @Test
        void lastHour() {
            MicrosFormatUtils.appendAmPm(sink, 23, EN);
            assertEquals("PM", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendEra
    // -----------------------------------------------------------------------
    @Nested
    class AppendEra {
        @Test
        void positiveYearIsAD() {
            MicrosFormatUtils.appendEra(sink, 2024, EN);
            assertEquals("AD", sink.toString());
        }

        @Test
        void yearOneIsAD() {
            MicrosFormatUtils.appendEra(sink, 1, EN);
            assertEquals("AD", sink.toString());
        }

        @Test
        void yearZeroIsAD() {
            MicrosFormatUtils.appendEra(sink, 0, EN);
            assertEquals("AD", sink.toString());
        }

        @Test
        void negativeYearIsBC() {
            MicrosFormatUtils.appendEra(sink, -1, EN);
            assertEquals("BC", sink.toString());
        }

        @Test
        void largeNegativeYear() {
            MicrosFormatUtils.appendEra(sink, -5000, EN);
            assertEquals("BC", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendHour12 -- 0-based 12-hour clock (0-11)
    // -----------------------------------------------------------------------
    @Nested
    class AppendHour12 {
        @Test
        void midnight() {
            MicrosFormatUtils.appendHour12(sink, 0);
            assertEquals("0", sink.toString());
        }

        @Test
        void oneAm() {
            MicrosFormatUtils.appendHour12(sink, 1);
            assertEquals("1", sink.toString());
        }

        @Test
        void elevenAm() {
            MicrosFormatUtils.appendHour12(sink, 11);
            assertEquals("11", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendHour12(sink, 12);
            assertEquals("0", sink.toString());
        }

        @Test
        void onePm() {
            MicrosFormatUtils.appendHour12(sink, 13);
            assertEquals("1", sink.toString());
        }

        @Test
        void elevenPm() {
            MicrosFormatUtils.appendHour12(sink, 23);
            assertEquals("11", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendHour12Padded -- 0-based 12-hour clock, padded to 2 digits
    // -----------------------------------------------------------------------
    @Nested
    class AppendHour12Padded {
        @Test
        void midnight() {
            MicrosFormatUtils.appendHour12Padded(sink, 0);
            assertEquals("00", sink.toString());
        }

        @Test
        void oneAm() {
            MicrosFormatUtils.appendHour12Padded(sink, 1);
            assertEquals("01", sink.toString());
        }

        @Test
        void elevenAm() {
            MicrosFormatUtils.appendHour12Padded(sink, 11);
            assertEquals("11", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendHour12Padded(sink, 12);
            assertEquals("00", sink.toString());
        }

        @Test
        void onePm() {
            MicrosFormatUtils.appendHour12Padded(sink, 13);
            assertEquals("01", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendHour121 -- 1-based 12-hour clock (1-12), no padding
    // -----------------------------------------------------------------------
    @Nested
    class AppendHour121 {
        @Test
        void midnight() {
            MicrosFormatUtils.appendHour121(sink, 0);
            assertEquals("12", sink.toString());
        }

        @Test
        void oneAm() {
            MicrosFormatUtils.appendHour121(sink, 1);
            assertEquals("1", sink.toString());
        }

        @Test
        void elevenAm() {
            MicrosFormatUtils.appendHour121(sink, 11);
            assertEquals("11", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendHour121(sink, 12);
            assertEquals("12", sink.toString());
        }

        @Test
        void onePm() {
            MicrosFormatUtils.appendHour121(sink, 13);
            assertEquals("1", sink.toString());
        }

        @Test
        void elevenPm() {
            MicrosFormatUtils.appendHour121(sink, 23);
            assertEquals("11", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendHour121Padded -- 1-based 12-hour clock, padded to 2 digits
    // -----------------------------------------------------------------------
    @Nested
    class AppendHour121Padded {
        @Test
        void midnight() {
            MicrosFormatUtils.appendHour121Padded(sink, 0);
            assertEquals("12", sink.toString());
        }

        @Test
        void oneAm() {
            MicrosFormatUtils.appendHour121Padded(sink, 1);
            assertEquals("01", sink.toString());
        }

        @Test
        void elevenAm() {
            MicrosFormatUtils.appendHour121Padded(sink, 11);
            assertEquals("11", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendHour121Padded(sink, 12);
            assertEquals("12", sink.toString());
        }

        @Test
        void onePm() {
            MicrosFormatUtils.appendHour121Padded(sink, 13);
            assertEquals("01", sink.toString());
        }

        @Test
        void nineAm() {
            MicrosFormatUtils.appendHour121Padded(sink, 9);
            assertEquals("09", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendHour241 -- 1-based 24-hour clock (1-24), no padding
    // -----------------------------------------------------------------------
    @Nested
    class AppendHour241 {
        @Test
        void midnight() {
            MicrosFormatUtils.appendHour241(sink, 0);
            assertEquals("24", sink.toString());
        }

        @Test
        void oneAm() {
            MicrosFormatUtils.appendHour241(sink, 1);
            assertEquals("1", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendHour241(sink, 12);
            assertEquals("12", sink.toString());
        }

        @Test
        void elevenPm() {
            MicrosFormatUtils.appendHour241(sink, 23);
            assertEquals("23", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // appendHour241Padded -- 1-based 24-hour clock, padded to 2 digits
    // -----------------------------------------------------------------------
    @Nested
    class AppendHour241Padded {
        @Test
        void midnight() {
            MicrosFormatUtils.appendHour241Padded(sink, 0);
            assertEquals("24", sink.toString());
        }

        @Test
        void oneAm() {
            MicrosFormatUtils.appendHour241Padded(sink, 1);
            assertEquals("01", sink.toString());
        }

        @Test
        void nine() {
            MicrosFormatUtils.appendHour241Padded(sink, 9);
            assertEquals("09", sink.toString());
        }

        @Test
        void noon() {
            MicrosFormatUtils.appendHour241Padded(sink, 12);
            assertEquals("12", sink.toString());
        }

        @Test
        void elevenPm() {
            MicrosFormatUtils.appendHour241Padded(sink, 23);
            assertEquals("23", sink.toString());
        }
    }

    // -----------------------------------------------------------------------
    // assertChar
    // -----------------------------------------------------------------------
    @Nested
    class AssertChar {
        @Test
        void matchingChar() throws NumericException {
            MicrosFormatUtils.assertChar('T', "2024-01-01T10:00:00", 10, 19);
            // no exception means success
        }

        @Test
        void nonMatchingCharThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertChar('T', "2024-01-01 10:00:00", 10, 19));
        }

        @Test
        void posAtHiThrows() {
            // pos == hi means no remaining chars
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertChar('X', "abc", 3, 3));
        }

        @Test
        void posBeyondHiThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertChar('a', "abc", 5, 3));
        }

        @Test
        void firstCharMatch() throws NumericException {
            MicrosFormatUtils.assertChar('a', "abc", 0, 3);
        }

        @Test
        void lastCharMatch() throws NumericException {
            MicrosFormatUtils.assertChar('c', "abc", 2, 3);
        }
    }

    // -----------------------------------------------------------------------
    // assertRemaining
    // -----------------------------------------------------------------------
    @Nested
    class AssertRemaining {
        @Test
        void posLessThanHiSucceeds() throws NumericException {
            MicrosFormatUtils.assertRemaining(0, 5);
        }

        @Test
        void posEqualsHiThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertRemaining(5, 5));
        }

        @Test
        void posBeyondHiThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertRemaining(10, 5));
        }
    }

    // -----------------------------------------------------------------------
    // assertNoTail
    // -----------------------------------------------------------------------
    @Nested
    class AssertNoTail {
        @Test
        void posEqualsHiSucceeds() throws NumericException {
            MicrosFormatUtils.assertNoTail(5, 5);
        }

        @Test
        void posBeyondHiSucceeds() throws NumericException {
            MicrosFormatUtils.assertNoTail(10, 5);
        }

        @Test
        void posLessThanHiThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertNoTail(3, 5));
        }
    }

    // -----------------------------------------------------------------------
    // assertString
    // -----------------------------------------------------------------------
    @Nested
    class AssertString {
        @Test
        void matchingDelimiter() throws NumericException {
            int result = MicrosFormatUtils.assertString("--", 2, "2024--01", 4, 8);
            assertEquals(6, result);
        }

        @Test
        void nonMatchingDelimiterThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertString("--", 2, "2024..01", 4, 8));
        }

        @Test
        void quotedDelimiter() throws NumericException {
            // quoted delimiters strip the surrounding quotes
            int result = MicrosFormatUtils.assertString("'at'", 4, "hello at world", 6, 14);
            assertEquals(8, result);
        }

        @Test
        void quotedDelimiterNoMatch() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertString("'at'", 4, "hello xx world", 6, 14));
        }

        @Test
        void notEnoughRemainingThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.assertString("long", 4, "ab", 0, 2));
        }
    }

    // -----------------------------------------------------------------------
    // compute -- constructs a micros timestamp from date/time components
    // -----------------------------------------------------------------------
    @Nested
    class Compute {
        @Test
        void epoch() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 1970, 1, -1, 1, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(0, micros);
        }

        @Test
        void simpleDate() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 3, -1, 15, 10, 30, 45, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(2024, Micros.getYear(micros));
            assertEquals(3, Micros.getMonthOfYear(micros, 2024, true));
            assertEquals(15, Micros.getDayOfMonth(micros, 2024, 3, true));
            assertEquals(10, Micros.getHourOfDay(micros));
            assertEquals(30, Micros.getMinuteOfHour(micros));
            assertEquals(45, Micros.getSecondOfMinute(micros));
        }

        @Test
        void withMillisAndMicros() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, 0, 0, 123, 456, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(123, Micros.getMillisOfSecond(micros));
            assertEquals(456, Micros.getMicrosOfMilli(micros));
        }

        @Test
        void bcEra() throws NumericException {
            // era=0 means BC, year is negated as -(year-1)
            long micros = MicrosFormatUtils.compute(
                    EN, 0, 500, 6, -1, 15, 12, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            int year = Micros.getYear(micros);
            assertTrue(year < 0, "BC era should produce negative year, got " + year);
        }

        @Test
        void hour12Am() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 9, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_AM
            );
            assertEquals(9, Micros.getHourOfDay(micros));
        }

        @Test
        void hour12Pm() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 3, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_PM
            );
            // PM adds 12 to the hour
            assertEquals(15, Micros.getHourOfDay(micros));
        }

        @Test
        void hour12NoonPm() throws NumericException {
            // hour=12 with PM: 12 % 12 = 0, + 12 = 12
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 12, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_PM
            );
            assertEquals(12, Micros.getHourOfDay(micros));
        }

        @Test
        void hour12MidnightAm() throws NumericException {
            // hour=12 with AM: 12 % 12 = 0
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 12, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_AM
            );
            assertEquals(0, Micros.getHourOfDay(micros));
        }

        @Test
        void hour24Midnight() throws NumericException {
            // hour=24 with 24-hour clock: 24 % 24 = 0
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 24, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(0, Micros.getHourOfDay(micros));
        }

        @Test
        void withOffset() throws NumericException {
            // offset in minutes; 60 minutes = 1 hour ahead
            long microsNoOffset = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 12, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            long microsWithOffset = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 12, 0, 0, 0, 0, -1, 60, CommonUtils.HOUR_24
            );
            assertEquals(microsNoOffset - 60 * Micros.MINUTE_MICROS, microsWithOffset);
        }

        @Test
        void leapYearFeb29() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 2, -1, 29, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(29, Micros.getDayOfMonth(micros, 2024, 2, true));
        }

        @Test
        void invalidMonthZeroThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 0, -1, 1, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidMonth13Throws() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 13, -1, 1, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidDayZeroThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 0, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidDay32Throws() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 32, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidFeb29NonLeapThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2023, 2, -1, 29, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidHour25Throws() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 25, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidNegativeHourThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, -1, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidMinute60Throws() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, 60, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidSecond60Throws() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, 0, 60, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidNegativeMinuteThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, -1, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidNegativeSecondThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, 0, -1, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void invalidHour13In12HourClockThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 13, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_AM
            ));
        }

        @Test
        void invalidWeekZeroThrows() {
            assertThrows(NumericException.class, () -> MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, 0, 1, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            ));
        }

        @Test
        void validIsoWeek() throws NumericException {
            // week 1 of 2024 should produce a valid timestamp
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, 1, 1, 0, 0, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            // the result is a valid timestamp in late Dec 2023 or early Jan 2024
            int year = Micros.getYear(micros);
            assertTrue(year == 2023 || year == 2024,
                    "ISO week 1 of 2024 should resolve to year 2023 or 2024, got " + year);
        }

        @Test
        void boundsMinute59() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, 59, 0, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(59, Micros.getMinuteOfHour(micros));
        }

        @Test
        void boundsSecond59() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 1, -1, 1, 0, 0, 59, 0, 0, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(59, Micros.getSecondOfMinute(micros));
        }

        @Test
        void decemberDate() throws NumericException {
            long micros = MicrosFormatUtils.compute(
                    EN, 1, 2024, 12, -1, 31, 23, 59, 59, 999, 999, -1, Long.MIN_VALUE, CommonUtils.HOUR_24
            );
            assertEquals(2024, Micros.getYear(micros));
            assertEquals(12, Micros.getMonthOfYear(micros, 2024, true));
            assertEquals(31, Micros.getDayOfMonth(micros, 2024, 12, true));
            assertEquals(23, Micros.getHourOfDay(micros));
            assertEquals(59, Micros.getMinuteOfHour(micros));
            assertEquals(59, Micros.getSecondOfMinute(micros));
            assertEquals(999, Micros.getMillisOfSecond(micros));
            assertEquals(999, Micros.getMicrosOfMilli(micros));
        }
    }

    // -----------------------------------------------------------------------
    // parseYearGreedy
    // -----------------------------------------------------------------------
    @Nested
    class ParseYearGreedy {
        @Test
        void fourDigitYear() throws NumericException {
            long result = MicrosFormatUtils.parseYearGreedy("2024", 0, 4);
            assertEquals(2024, Numbers.decodeLowInt(result));
            assertEquals(4, Numbers.decodeHighInt(result));
        }

        @Test
        void twoDigitYearGetsAdjusted() throws NumericException {
            long result = MicrosFormatUtils.parseYearGreedy("24", 0, 2);
            int year = Numbers.decodeLowInt(result);
            int len = Numbers.decodeHighInt(result);
            assertEquals(2, len);
            assertEquals(MicrosFormatUtils.adjustYear(24), year);
        }

        @Test
        void singleDigitNotAdjusted() throws NumericException {
            long result = MicrosFormatUtils.parseYearGreedy("5", 0, 1);
            int year = Numbers.decodeLowInt(result);
            int len = Numbers.decodeHighInt(result);
            assertEquals(1, len);
            assertEquals(5, year);
        }

        @Test
        void threeDigitNotAdjusted() throws NumericException {
            long result = MicrosFormatUtils.parseYearGreedy("324", 0, 3);
            int year = Numbers.decodeLowInt(result);
            int len = Numbers.decodeHighInt(result);
            assertEquals(3, len);
            assertEquals(324, year);
        }

        @Test
        void yearInMiddleOfString() throws NumericException {
            // "xx2024yy" -- parse from pos 2 to 6
            long result = MicrosFormatUtils.parseYearGreedy("xx2024yy", 2, 6);
            assertEquals(2024, Numbers.decodeLowInt(result));
            assertEquals(4, Numbers.decodeHighInt(result));
        }

        @Test
        void invalidInputThrows() {
            assertThrows(NumericException.class,
                    () -> MicrosFormatUtils.parseYearGreedy("abcd", 0, 4));
        }
    }
}
