package io.questdb.kafka.compat;

import io.questdb.client.std.Numbers;
import io.questdb.kafka.compat.datetime.millitime.Dates;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link Dates} (millisecond timestamp arithmetic).
 */
public class DatesTest {

    // =========================================================================
    // Constants
    // =========================================================================

    @Test
    void constants() {
        assertEquals(86_400_000L, Dates.DAY_MILLIS);
        assertEquals(3_600_000L, Dates.HOUR_MILLIS);
        assertEquals(60_000L, Dates.MINUTE_MILLIS);
        assertEquals(1_000L, Dates.SECOND_MILLIS);
    }

    // =========================================================================
    // parseOffset
    // =========================================================================

    @Test
    void parseOffset_plusFiveThirty() {
        long result = Dates.parseOffset("+05:30");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(330, minutes); // 5*60+30
    }

    @Test
    void parseOffset_minusEight() {
        long result = Dates.parseOffset("-08:00");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(-480, minutes); // -(8*60)
    }

    @Test
    void parseOffset_zero() {
        long result = Dates.parseOffset("+00:00");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(0, minutes);
    }

    @Test
    void parseOffset_utcPrefix() {
        long result = Dates.parseOffset("UTC+03:00");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(180, minutes); // 3*60
    }

    @Test
    void parseOffset_gmtPrefix() {
        long result = Dates.parseOffset("GMT-05:30");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(-(5 * 60 + 30), minutes);
    }

    @Test
    void parseOffset_zulu() {
        long result = Dates.parseOffset("Z");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(0, minutes);
    }

    @Test
    void parseOffset_withoutColon() {
        long result = Dates.parseOffset("+0530");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(330, minutes);
    }

    @Test
    void parseOffset_bareDigits() {
        long result = Dates.parseOffset("0530");
        assertNotEquals(Long.MIN_VALUE, result);
        int minutes = Numbers.decodeLowInt(result);
        assertEquals(330, minutes);
    }

    @Test
    void parseOffset_invalid_notOffset() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("not_offset"));
    }

    @Test
    void parseOffset_invalid_emptyString() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset(""));
    }

    @Test
    void parseOffset_invalid_hourTooHigh() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("+25:00"));
    }

    @Test
    void parseOffset_invalid_minuteTooHigh() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("+05:60"));
    }

    @Test
    void parseOffset_invalid_utcWithoutSign() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("UTC"));
    }

    @Test
    void parseOffset_caseInsensitive_utc() {
        long result = Dates.parseOffset("utc+01:00");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(60, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_caseInsensitive_gmt() {
        long result = Dates.parseOffset("gmt-02:00");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(-120, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_caseInsensitive_z() {
        long result = Dates.parseOffset("z");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(0, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_substringOverload() {
        long result = Dates.parseOffset("xxx+05:30yyy", 3, 9);
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(330, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_singleArgMatchesThreeArgOverload() {
        long r1 = Dates.parseOffset("+05:30");
        long r2 = Dates.parseOffset("+05:30", 0, 6);
        assertEquals(Numbers.decodeLowInt(r1), Numbers.decodeLowInt(r2));
    }

    @Test
    void parseOffset_lengthConsumed() {
        long result = Dates.parseOffset("+05:30");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(6, Numbers.decodeHighInt(result));
    }

    @Test
    void parseOffset_utcLengthConsumed() {
        long result = Dates.parseOffset("UTC+05:30");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(9, Numbers.decodeHighInt(result));
    }

    @Test
    void parseOffset_zuluLengthConsumed() {
        long result = Dates.parseOffset("Z");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(1, Numbers.decodeHighInt(result));
    }

    @Test
    void parseOffset_invalid_gmtPartial() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("GX+05:00"));
    }

    @Test
    void parseOffset_invalid_utcPartial() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("UA+05:00"));
    }

    @Test
    void parseOffset_hourOnly_noMinutes() {
        // "+05" ends at DELIM state, hour=5, minute=0
        long result = Dates.parseOffset("+05");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(300, Numbers.decodeLowInt(result)); // 5*60
    }

    @Test
    void parseOffset_invalid_singleDigitHour() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("+5"));
    }

    @Test
    void parseOffset_negativeZero() {
        // -00:00 parses, negative flag is set but min=0 so result is 0
        long result = Dates.parseOffset("-00:00");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(0, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_maxValidOffset() {
        long result = Dates.parseOffset("+23:59");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(23 * 60 + 59, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_minValidOffset() {
        long result = Dates.parseOffset("-23:59");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(-(23 * 60 + 59), Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_gmtPlusZero() {
        long result = Dates.parseOffset("GMT+00:00");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(0, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_invalid_onlySign() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("+"));
    }

    @Test
    void parseOffset_invalid_abc() {
        assertEquals(Long.MIN_VALUE, Dates.parseOffset("abc"));
    }

    @Test
    void parseOffset_bareDigitsWithColon() {
        long result = Dates.parseOffset("05:30");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(330, Numbers.decodeLowInt(result));
    }

    @Test
    void parseOffset_gmtNegative() {
        long result = Dates.parseOffset("GMT-12:00");
        assertNotEquals(Long.MIN_VALUE, result);
        assertEquals(-720, Numbers.decodeLowInt(result));
    }
}
