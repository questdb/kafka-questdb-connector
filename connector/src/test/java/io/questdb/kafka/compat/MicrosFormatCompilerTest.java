package io.questdb.kafka.compat;

import io.questdb.client.std.NumericException;
import io.questdb.kafka.compat.datetime.DateFormat;
import io.questdb.kafka.compat.datetime.DateLocaleFactory;
import io.questdb.kafka.compat.datetime.microtime.MicrosFormatCompiler;
import io.questdb.kafka.compat.datetime.microtime.Micros;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the bytecode-compiled DateFormat implementations work correctly
 * after the package migration from io.questdb.std to io.questdb.kafka.compat.
 * <p>
 * The MicrosFormatCompiler generates JVM bytecode at runtime with hardcoded
 * class references. These tests verify that the rewritten package paths
 * in method descriptors are correct and produce working DateFormat instances.
 */
public class MicrosFormatCompilerTest {
    private static MicrosFormatCompiler compiler;

    @BeforeAll
    static void setUp() {
        compiler = new MicrosFormatCompiler();
    }

    @Test
    void testCompileBasicPattern() {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        assertNotNull(fmt, "compiled DateFormat must not be null");
    }

    @Test
    void testParseIsoTimestamp() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        long micros = fmt.parse("2024-03-15T10:30:45.123Z", DateLocaleFactory.EN_LOCALE);
        // verify year
        assertEquals(2024, Micros.getYear(micros));
        // verify month
        assertEquals(3, Micros.getMonthOfYear(micros, 2024, io.questdb.kafka.compat.datetime.CommonUtils.isLeapYear(2024)));
        // verify day
        assertEquals(15, Micros.getDayOfMonth(micros, 2024, 3, io.questdb.kafka.compat.datetime.CommonUtils.isLeapYear(2024)));
        // verify hour
        assertEquals(10, Micros.getHourOfDay(micros));
        // verify minute
        assertEquals(30, Micros.getMinuteOfHour(micros));
        // verify second
        assertEquals(45, Micros.getSecondOfMinute(micros));
        // verify millis
        assertEquals(123, Micros.getMillisOfSecond(micros));
    }

    @Test
    void testParseMicrosecondTimestamp() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUz");
        long micros = fmt.parse("2024-03-15T10:30:45.123456Z", DateLocaleFactory.EN_LOCALE);
        assertEquals(2024, Micros.getYear(micros));
        assertEquals(456, Micros.getMicrosOfMilli(micros));
        assertEquals(123, Micros.getMillisOfSecond(micros));
    }

    @Test
    void testParseRoundTrip() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUz");
        String input = "2024-03-15T10:30:45.123456Z";
        long micros = fmt.parse(input, DateLocaleFactory.EN_LOCALE);

        // format back and parse again - must get the same micros
        StringBuilder sb = new StringBuilder();
        io.questdb.client.std.str.StringSink sink = new io.questdb.client.std.str.StringSink();
        fmt.format(micros, DateLocaleFactory.EN_LOCALE, "Z", sink);
        long micros2 = fmt.parse(sink, DateLocaleFactory.EN_LOCALE);
        assertEquals(micros, micros2, "round-trip must preserve microsecond value");
    }

    @Test
    void testFormatOutput() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        long micros = fmt.parse("2024-03-15T10:30:45.123Z", DateLocaleFactory.EN_LOCALE);

        io.questdb.client.std.str.StringSink sink = new io.questdb.client.std.str.StringSink();
        fmt.format(micros, DateLocaleFactory.EN_LOCALE, "Z", sink);
        assertEquals("2024-03-15T10:30:45.123Z", sink.toString());
    }

    @Test
    void testParseDateOnly() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-dd");
        long micros = fmt.parse("2024-03-15", DateLocaleFactory.EN_LOCALE);
        assertEquals(2024, Micros.getYear(micros));
        assertEquals(3, Micros.getMonthOfYear(micros, 2024, io.questdb.kafka.compat.datetime.CommonUtils.isLeapYear(2024)));
        assertEquals(15, Micros.getDayOfMonth(micros, 2024, 3, io.questdb.kafka.compat.datetime.CommonUtils.isLeapYear(2024)));
        assertEquals(0, Micros.getHourOfDay(micros));
    }

    @Test
    void testParseWithTimezone() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ssz");
        // +05:30 offset means UTC time is 5:30 earlier
        long micros = fmt.parse("2024-03-15T10:30:45+05:30", DateLocaleFactory.EN_LOCALE);
        assertEquals(5, Micros.getHourOfDay(micros));
        assertEquals(0, Micros.getMinuteOfHour(micros));
    }

    @Test
    void testInvalidInputThrowsNumericException() {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        assertThrows(NumericException.class, () -> fmt.parse("not-a-date", DateLocaleFactory.EN_LOCALE));
    }

    @Test
    void testMultiplePatternsCompile() {
        // verify that multiple different patterns can be compiled and used
        String[] patterns = {
                "yyyy-MM-ddTHH:mm:ss.SSSz",
                "yyyy-MM-ddTHH:mm:ss.SSSUUUz",
                "dd/MM/yyyy HH:mm:ss",
                "yyyy-MM-dd",
                "yyyyMMdd",
                "dd-MMM-yyyy",
        };
        for (String pattern : patterns) {
            DateFormat fmt = compiler.compile(pattern);
            assertNotNull(fmt, "pattern '" + pattern + "' must compile");
        }
    }

    @Test
    void testCompileAndParseMultipleInstances() throws NumericException {
        // verify two separately compiled formats work independently
        DateFormat fmt1 = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUz");
        DateFormat fmt2 = compiler.compile("dd/MM/yyyy HH:mm:ss");
        long micros1 = fmt1.parse("2024-01-01T00:00:00.000000Z", DateLocaleFactory.EN_LOCALE);
        long micros2 = fmt2.parse("01/01/2024 00:00:00", DateLocaleFactory.EN_LOCALE);
        assertEquals(micros1, micros2);
    }

    @Test
    void testEpochParse() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        long micros = fmt.parse("1970-01-01T00:00:00.000Z", DateLocaleFactory.EN_LOCALE);
        assertEquals(0, micros);
    }

    @Test
    void testLeapYearDate() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-dd");
        long micros = fmt.parse("2024-02-29", DateLocaleFactory.EN_LOCALE);
        assertEquals(2024, Micros.getYear(micros));
        assertEquals(2, Micros.getMonthOfYear(micros, 2024, true));
        assertEquals(29, Micros.getDayOfMonth(micros, 2024, 2, true));
    }

    @Test
    void testNegativeTimestamp() throws NumericException {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        long micros = fmt.parse("1969-12-31T23:59:59.999Z", DateLocaleFactory.EN_LOCALE);
        assertTrue(micros < 0, "pre-epoch timestamp must be negative");
    }

    @Test
    void testGetColumnType() {
        DateFormat fmt = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSz");
        assertEquals(io.questdb.client.cairo.ColumnType.TIMESTAMP_MICRO, fmt.getColumnType());
    }
}
