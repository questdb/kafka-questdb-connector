package io.questdb.kafka.compat;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.kafka.compat.datetime.CommonUtils;
import io.questdb.kafka.compat.datetime.microtime.Micros;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the compat copy of Micros (timestamp arithmetic and decomposition).
 */
public class MicrosTest {

    // ========================= Constants =========================

    @Test
    void testConstants() {
        assertEquals(86_400_000_000L, Micros.DAY_MICROS);
        assertEquals(3_600_000_000L, Micros.HOUR_MICROS);
        assertEquals(60_000_000L, Micros.MINUTE_MICROS);
        assertEquals(1_000_000L, Micros.SECOND_MICROS);
        assertEquals(1_000L, Micros.MILLI_MICROS);
        assertEquals(1_000L, Micros.MICRO_NANOS);
        assertEquals(1_000, Micros.SECOND_MILLIS);
        assertEquals(60L, Micros.MINUTE_SECONDS);
        assertEquals(7 * Micros.DAY_MICROS, Micros.WEEK_MICROS);
        assertEquals(365 * Micros.DAY_MICROS, Micros.YEAR_MICROS_NONLEAP);
    }

    // ========================= getYear =========================

    @Test
    void testGetYearEpoch() {
        assertEquals(1970, Micros.getYear(0));
    }

    @Test
    void testGetYearOneSecondBeforeEpoch() {
        assertEquals(1969, Micros.getYear(-Micros.SECOND_MICROS));
    }

    @Test
    void testGetYearOneMicroBeforeEpoch() {
        assertEquals(1969, Micros.getYear(-1));
    }

    @Test
    void testGetYearY2K() {
        long y2k = Micros.yearMicros(2000, CommonUtils.isLeapYear(2000));
        assertEquals(2000, Micros.getYear(y2k));
    }

    @Test
    void testGetYear2024LeapYear() {
        long y2024 = Micros.yearMicros(2024, CommonUtils.isLeapYear(2024));
        assertEquals(2024, Micros.getYear(y2024));
        // also at end of year
        long lastMicro = y2024 + 366 * Micros.DAY_MICROS - 1;
        assertEquals(2024, Micros.getYear(lastMicro));
    }

    @Test
    void testGetYear1900NonLeap() {
        assertFalse(CommonUtils.isLeapYear(1900));
        long y1900 = Micros.yearMicros(1900, false);
        assertEquals(1900, Micros.getYear(y1900));
    }

    @Test
    void testGetYearPreEpoch1969() {
        long y1969 = Micros.yearMicros(1969, CommonUtils.isLeapYear(1969));
        assertEquals(1969, Micros.getYear(y1969));
    }

    @Test
    void testGetYearPreEpoch1800() {
        long y1800 = Micros.yearMicros(1800, CommonUtils.isLeapYear(1800));
        assertEquals(1800, Micros.getYear(y1800));
    }

    @Test
    void testGetYearPreEpoch1888() {
        long y1888 = Micros.yearMicros(1888, CommonUtils.isLeapYear(1888));
        assertEquals(1888, Micros.getYear(y1888));
    }

    @Test
    void testGetYearFarFuture() {
        long y3000 = Micros.yearMicros(3000, CommonUtils.isLeapYear(3000));
        assertEquals(3000, Micros.getYear(y3000));
    }

    @Test
    void testGetYearRoundTrip() {
        // For a wide range of years, check that yearMicros -> getYear round-trips correctly
        for (int y = 1600; y <= 2400; y += 10) {
            boolean leap = CommonUtils.isLeapYear(y);
            long micros = Micros.yearMicros(y, leap);
            assertEquals(y, Micros.getYear(micros), "Year round-trip failed for " + y);
        }
    }

    // ========================= getMonthOfYear =========================

    @Test
    void testGetMonthOfYearAllMonths() {
        int year = 2023;
        boolean leap = CommonUtils.isLeapYear(year);
        assertFalse(leap);
        for (int month = 1; month <= 12; month++) {
            long micros = Micros.toMicros(year, leap, month, 1, 0, 0);
            assertEquals(month, Micros.getMonthOfYear(micros, year, leap),
                    "Month mismatch for month=" + month);
        }
    }

    @Test
    void testGetMonthOfYearAllMonthsLeapYear() {
        int year = 2024;
        boolean leap = CommonUtils.isLeapYear(year);
        assertTrue(leap);
        for (int month = 1; month <= 12; month++) {
            long micros = Micros.toMicros(year, leap, month, 1, 0, 0);
            assertEquals(month, Micros.getMonthOfYear(micros, year, leap),
                    "Leap year month mismatch for month=" + month);
        }
    }

    @Test
    void testGetMonthOfYearLastDayOfEachMonth() {
        int year = 2023;
        boolean leap = false;
        int[] daysInMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        for (int month = 1; month <= 12; month++) {
            long micros = Micros.toMicros(year, leap, month, daysInMonth[month - 1], 23, 59);
            assertEquals(month, Micros.getMonthOfYear(micros, year, leap),
                    "Last day of month mismatch for month=" + month);
        }
    }

    @Test
    void testGetMonthOfYearFebLeapYearDay29() {
        int year = 2024;
        boolean leap = true;
        long feb29 = Micros.toMicros(year, leap, 2, 29, 12, 0);
        assertEquals(2, Micros.getMonthOfYear(feb29, year, leap));
    }

    @Test
    void testGetMonthOfYearSingleArgOverload() {
        // Test the overload that computes year and leap internally
        long jan1_2023 = Micros.toMicros(2023, false, 1, 1, 0, 0);
        assertEquals(1, Micros.getMonthOfYear(jan1_2023));
        long dec31_2023 = Micros.toMicros(2023, false, 12, 31, 23, 59);
        assertEquals(12, Micros.getMonthOfYear(dec31_2023));
    }

    @Test
    void testGetMonthOfYearPreEpoch() {
        int year = 1969;
        boolean leap = CommonUtils.isLeapYear(year);
        long july1969 = Micros.toMicros(year, leap, 7, 20, 20, 17);
        assertEquals(7, Micros.getMonthOfYear(july1969, year, leap));
    }

    // ========================= getDayOfMonth =========================

    @Test
    void testGetDayOfMonthFirstDayOfEachMonth() {
        int year = 2023;
        boolean leap = false;
        for (int month = 1; month <= 12; month++) {
            long micros = Micros.toMicros(year, leap, month, 1, 0, 0);
            assertEquals(1, Micros.getDayOfMonth(micros, year, month, leap),
                    "First day mismatch for month=" + month);
        }
    }

    @Test
    void testGetDayOfMonthLastDayOfEachMonth() {
        int year = 2023;
        boolean leap = false;
        int[] daysInMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        for (int month = 1; month <= 12; month++) {
            int lastDay = daysInMonth[month - 1];
            long micros = Micros.toMicros(year, leap, month, lastDay, 0, 0);
            assertEquals(lastDay, Micros.getDayOfMonth(micros, year, month, leap),
                    "Last day mismatch for month=" + month);
        }
    }

    @Test
    void testGetDayOfMonthFeb29LeapYear() {
        int year = 2024;
        boolean leap = true;
        long feb29 = Micros.toMicros(year, leap, 2, 29, 0, 0);
        assertEquals(29, Micros.getDayOfMonth(feb29, year, 2, leap));
    }

    @Test
    void testGetDayOfMonthMidMonth() {
        int year = 2023;
        boolean leap = false;
        long mar15 = Micros.toMicros(year, leap, 3, 15, 0, 0);
        assertEquals(15, Micros.getDayOfMonth(mar15, year, 3, leap));
    }

    // ========================= getHourOfDay =========================

    @Test
    void testGetHourOfDayEpoch() {
        assertEquals(0, Micros.getHourOfDay(0));
    }

    @Test
    void testGetHourOfDayAllHours() {
        for (int h = 0; h < 24; h++) {
            long micros = h * Micros.HOUR_MICROS;
            assertEquals(h, Micros.getHourOfDay(micros), "Hour mismatch for h=" + h);
        }
    }

    @Test
    void testGetHourOfDayNegative() {
        // One hour before epoch = 23:00 on Dec 31 1969
        assertEquals(23, Micros.getHourOfDay(-Micros.HOUR_MICROS));
        // 1 microsecond before epoch = still 23:59:59.999999
        assertEquals(23, Micros.getHourOfDay(-1));
    }

    @Test
    void testGetHourOfDayPreEpoch() {
        // Construct a known pre-epoch time: 1969-07-20 20:17:00
        int year = 1969;
        boolean leap = CommonUtils.isLeapYear(year);
        long micros = Micros.toMicros(year, leap, 7, 20, 20, 17);
        assertEquals(20, Micros.getHourOfDay(micros));
    }

    @Test
    void testGetHourOfDayMidnight() {
        // Midnight of 2023-06-15
        long micros = Micros.toMicros(2023, false, 6, 15, 0, 0);
        assertEquals(0, Micros.getHourOfDay(micros));
    }

    @Test
    void testGetHourOfDayNoon() {
        long micros = Micros.toMicros(2023, false, 6, 15, 12, 0);
        assertEquals(12, Micros.getHourOfDay(micros));
    }

    // ========================= getMinuteOfHour =========================

    @Test
    void testGetMinuteOfHourEpoch() {
        assertEquals(0, Micros.getMinuteOfHour(0));
    }

    @Test
    void testGetMinuteOfHourAllMinutes() {
        for (int m = 0; m < 60; m++) {
            long micros = m * Micros.MINUTE_MICROS;
            assertEquals(m, Micros.getMinuteOfHour(micros), "Minute mismatch for m=" + m);
        }
    }

    @Test
    void testGetMinuteOfHourNegative() {
        // 1 minute before epoch = 23:59
        assertEquals(59, Micros.getMinuteOfHour(-Micros.MINUTE_MICROS));
    }

    @Test
    void testGetMinuteOfHourKnownTimestamp() {
        long micros = Micros.toMicros(2023, false, 3, 15, 14, 37);
        assertEquals(37, Micros.getMinuteOfHour(micros));
    }

    // ========================= getSecondOfMinute =========================

    @Test
    void testGetSecondOfMinuteEpoch() {
        assertEquals(0, Micros.getSecondOfMinute(0));
    }

    @Test
    void testGetSecondOfMinuteAllSeconds() {
        for (int s = 0; s < 60; s++) {
            long micros = s * Micros.SECOND_MICROS;
            assertEquals(s, Micros.getSecondOfMinute(micros), "Second mismatch for s=" + s);
        }
    }

    @Test
    void testGetSecondOfMinuteNegative() {
        // 1 second before epoch = xx:xx:59
        assertEquals(59, Micros.getSecondOfMinute(-Micros.SECOND_MICROS));
    }

    // ========================= getMillisOfSecond =========================

    @Test
    void testGetMillisOfSecondEpoch() {
        assertEquals(0, Micros.getMillisOfSecond(0));
    }

    @Test
    void testGetMillisOfSecondKnownValues() {
        assertEquals(1, Micros.getMillisOfSecond(Micros.MILLI_MICROS));      // 1ms
        assertEquals(500, Micros.getMillisOfSecond(500 * Micros.MILLI_MICROS)); // 500ms
        assertEquals(999, Micros.getMillisOfSecond(999 * Micros.MILLI_MICROS)); // 999ms
    }

    @Test
    void testGetMillisOfSecondWithMicrosComponent() {
        // 1500 micros = 1ms and 500 micros
        assertEquals(1, Micros.getMillisOfSecond(1500));
        // 999999 micros = 999ms and 999 micros
        assertEquals(999, Micros.getMillisOfSecond(999999));
    }

    @Test
    void testGetMillisOfSecondNegative() {
        // 1 millisecond before epoch
        assertEquals(999, Micros.getMillisOfSecond(-Micros.MILLI_MICROS));
    }

    // ========================= getMicrosOfMilli =========================

    @Test
    void testGetMicrosOfMilliEpoch() {
        assertEquals(0, Micros.getMicrosOfMilli(0));
    }

    @Test
    void testGetMicrosOfMilliKnownValues() {
        assertEquals(1, Micros.getMicrosOfMilli(1));
        assertEquals(500, Micros.getMicrosOfMilli(500));
        assertEquals(999, Micros.getMicrosOfMilli(999));
        assertEquals(0, Micros.getMicrosOfMilli(1000)); // exact millisecond boundary
    }

    @Test
    void testGetMicrosOfMilliPrecision() {
        // 1.5 ms = 1500 micros: millis=1, microsOfMilli=500
        assertEquals(500, Micros.getMicrosOfMilli(1500));
    }

    @Test
    void testGetMicrosOfMilliNegative() {
        assertEquals(999, Micros.getMicrosOfMilli(-1));
    }

    // ========================= getMicrosOfSecond =========================

    @Test
    void testGetMicrosOfSecondEpoch() {
        assertEquals(0, Micros.getMicrosOfSecond(0));
    }

    @Test
    void testGetMicrosOfSecondKnownValues() {
        assertEquals(1, Micros.getMicrosOfSecond(1));
        assertEquals(999999, Micros.getMicrosOfSecond(999999));
        assertEquals(0, Micros.getMicrosOfSecond(Micros.SECOND_MICROS));
        assertEquals(500000, Micros.getMicrosOfSecond(Micros.SECOND_MICROS + 500000));
    }

    @Test
    void testGetMicrosOfSecondNegative() {
        // 1 microsecond before epoch: 999999 micros within the second
        assertEquals(999999, Micros.getMicrosOfSecond(-1));
    }

    // ========================= getDayOfWeek =========================

    @Test
    void testGetDayOfWeekEpochIsThursday() {
        // 1970-01-01 was a Thursday
        // getDayOfWeek: 1=Monday ... 7=Sunday
        assertEquals(4, Micros.getDayOfWeek(0)); // Thursday = 4
    }

    @Test
    void testGetDayOfWeekFriday() {
        // 1970-01-02 is Friday
        assertEquals(5, Micros.getDayOfWeek(Micros.DAY_MICROS));
    }

    @Test
    void testGetDayOfWeekSaturday() {
        // 1970-01-03 is Saturday
        assertEquals(6, Micros.getDayOfWeek(2 * Micros.DAY_MICROS));
    }

    @Test
    void testGetDayOfWeekSunday() {
        // 1970-01-04 is Sunday
        assertEquals(7, Micros.getDayOfWeek(3 * Micros.DAY_MICROS));
    }

    @Test
    void testGetDayOfWeekMonday() {
        // 1970-01-05 is Monday
        assertEquals(1, Micros.getDayOfWeek(4 * Micros.DAY_MICROS));
    }

    @Test
    void testGetDayOfWeekFullWeekFromEpoch() {
        // Thu=4, Fri=5, Sat=6, Sun=7, Mon=1, Tue=2, Wed=3
        int[] expected = {4, 5, 6, 7, 1, 2, 3};
        for (int i = 0; i < 7; i++) {
            assertEquals(expected[i], Micros.getDayOfWeek(i * Micros.DAY_MICROS),
                    "Day of week mismatch for day offset " + i);
        }
    }

    @Test
    void testGetDayOfWeekPreEpoch() {
        // 1969-12-31 was a Wednesday (day before Thursday Jan 1 1970)
        assertEquals(3, Micros.getDayOfWeek(-Micros.DAY_MICROS));
        // 1969-12-30 was a Tuesday
        assertEquals(2, Micros.getDayOfWeek(-2 * Micros.DAY_MICROS));
        // 1969-12-29 was a Monday
        assertEquals(1, Micros.getDayOfWeek(-3 * Micros.DAY_MICROS));
        // 1969-12-28 was a Sunday
        assertEquals(7, Micros.getDayOfWeek(-4 * Micros.DAY_MICROS));
        // 1969-12-27 was a Saturday
        assertEquals(6, Micros.getDayOfWeek(-5 * Micros.DAY_MICROS));
        // 1969-12-26 was a Friday
        assertEquals(5, Micros.getDayOfWeek(-6 * Micros.DAY_MICROS));
        // 1969-12-25 was a Thursday
        assertEquals(4, Micros.getDayOfWeek(-7 * Micros.DAY_MICROS));
    }

    @Test
    void testGetDayOfWeekPreEpochOneMicroBeforeDay() {
        // 1 micro before epoch: still Dec 31 1969 (Wednesday)
        assertEquals(3, Micros.getDayOfWeek(-1));
    }

    @Test
    void testGetDayOfWeekKnownDate2023Dec25() {
        // 2023-12-25 was a Monday
        long micros = Micros.toMicros(2023, false, 12, 25, 0, 0);
        assertEquals(1, Micros.getDayOfWeek(micros));
    }

    // ========================= getDayOfWeekSundayFirst =========================

    @Test
    void testGetDayOfWeekSundayFirstEpochIsThursday() {
        // getDayOfWeekSundayFirst: 1=Sunday, 2=Monday ... 7=Saturday
        // Thursday = 5
        assertEquals(5, Micros.getDayOfWeekSundayFirst(0));
    }

    @Test
    void testGetDayOfWeekSundayFirstFullWeekFromEpoch() {
        // Thu=5, Fri=6, Sat=7, Sun=1, Mon=2, Tue=3, Wed=4
        int[] expected = {5, 6, 7, 1, 2, 3, 4};
        for (int i = 0; i < 7; i++) {
            assertEquals(expected[i], Micros.getDayOfWeekSundayFirst(i * Micros.DAY_MICROS),
                    "SundayFirst day of week mismatch for day offset " + i);
        }
    }

    @Test
    void testGetDayOfWeekSundayFirstPreEpoch() {
        // 1969-12-31 was Wednesday -> SundayFirst = 4
        assertEquals(4, Micros.getDayOfWeekSundayFirst(-Micros.DAY_MICROS));
        // 1969-12-28 was Sunday -> SundayFirst = 1
        assertEquals(1, Micros.getDayOfWeekSundayFirst(-4 * Micros.DAY_MICROS));
        // 1969-12-27 was Saturday -> SundayFirst = 7
        assertEquals(7, Micros.getDayOfWeekSundayFirst(-5 * Micros.DAY_MICROS));
    }

    @Test
    void testGetDayOfWeekSundayFirstOneMicroBeforeEpoch() {
        // Still Wednesday in SundayFirst convention -> 4
        assertEquals(4, Micros.getDayOfWeekSundayFirst(-1));
    }

    // ========================= getDayOfYear =========================

    @Test
    void testGetDayOfYearJan1() {
        long jan1_2023 = Micros.toMicros(2023, false, 1, 1, 0, 0);
        assertEquals(1, Micros.getDayOfYear(jan1_2023));
    }

    @Test
    void testGetDayOfYearDec31NonLeap() {
        long dec31_2023 = Micros.toMicros(2023, false, 12, 31, 0, 0);
        assertEquals(365, Micros.getDayOfYear(dec31_2023));
    }

    @Test
    void testGetDayOfYearDec31LeapYear() {
        long dec31_2024 = Micros.toMicros(2024, true, 12, 31, 0, 0);
        assertEquals(366, Micros.getDayOfYear(dec31_2024));
    }

    @Test
    void testGetDayOfYearMar1NonLeap() {
        // In non-leap year, March 1 = day 60
        long mar1_2023 = Micros.toMicros(2023, false, 3, 1, 0, 0);
        assertEquals(60, Micros.getDayOfYear(mar1_2023));
    }

    @Test
    void testGetDayOfYearMar1LeapYear() {
        // In leap year, March 1 = day 61
        long mar1_2024 = Micros.toMicros(2024, true, 3, 1, 0, 0);
        assertEquals(61, Micros.getDayOfYear(mar1_2024));
    }

    @Test
    void testGetDayOfYearPreEpoch() {
        // 1969-01-01 should be day 1
        long jan1_1969 = Micros.toMicros(1969, false, 1, 1, 0, 0);
        assertEquals(1, Micros.getDayOfYear(jan1_1969));
    }

    @Test
    void testGetDoyIsSameAsDayOfYear() {
        long micros = Micros.toMicros(2023, false, 6, 15, 12, 30);
        assertEquals(Micros.getDayOfYear(micros), Micros.getDoy(micros));
    }

    // ========================= getWeek (ISO) =========================

    @Test
    void testGetWeekJan1_2023() {
        // 2023-01-01 is a Sunday. ISO week: Jan 1 Sunday belongs to week 52 of previous year
        // Actually depends on exact ISO calculation. Let's compute.
        // 2023-01-01 is Sunday, getDayOfWeek=7, doy=1
        // w = (10 + 1 - 7) / 7 = 4/7 = 0 -> w < 1 -> getWeeks(2022)
        long jan1_2023 = Micros.toMicros(2023, false, 1, 1, 0, 0);
        int week = Micros.getWeek(jan1_2023);
        assertEquals(52, week); // Jan 1 2023 (Sunday) is in ISO week 52 of 2022
    }

    @Test
    void testGetWeekJan2_2023() {
        // 2023-01-02 is Monday -> start of ISO week 1
        long jan2_2023 = Micros.toMicros(2023, false, 1, 2, 0, 0);
        assertEquals(1, Micros.getWeek(jan2_2023));
    }

    @Test
    void testGetWeekMidYear() {
        // 2023-07-03 is Monday (week 27 by ISO calculation)
        // doy for July 3 non-leap: 31+28+31+30+31+30+3 = 184
        // dow = 1 (Monday)
        // w = (10 + 184 - 1) / 7 = 193/7 = 27
        long jul3_2023 = Micros.toMicros(2023, false, 7, 3, 0, 0);
        assertEquals(27, Micros.getWeek(jul3_2023));
    }

    @Test
    void testGetWeekDec31OfLongIsoYear() {
        // 2020 is a year that has 53 ISO weeks.
        // 2020-12-31 is Thursday, doy=366, dow=4
        // w = (10 + 366 - 4)/7 = 372/7 = 53
        // getWeeks(2020) = 53, so w=53 is valid
        long dec31_2020 = Micros.toMicros(2020, true, 12, 31, 0, 0);
        assertEquals(53, Micros.getWeek(dec31_2020));
    }

    @Test
    void testGetWeekDec31WrapsToWeek1() {
        // 2024 has 52 ISO weeks. 2024-12-31 is Tuesday.
        // doy=366 (leap year), dow=2
        // w = (10 + 366 - 2)/7 = 374/7 = 53
        // getWeeks(2024) = 52, so w > getWeeks(y) -> return 1
        long dec31_2024 = Micros.toMicros(2024, true, 12, 31, 0, 0);
        assertEquals(1, Micros.getWeek(dec31_2024));
    }

    // ========================= getWeekOfYear (simple) =========================

    @Test
    void testGetWeekOfYearJan1() {
        long jan1 = Micros.toMicros(2023, false, 1, 1, 0, 0);
        // dayOfYear=1, 1/7 + 1 = 1
        assertEquals(1, Micros.getWeekOfYear(jan1));
    }

    @Test
    void testGetWeekOfYearJan7() {
        long jan7 = Micros.toMicros(2023, false, 1, 7, 0, 0);
        // dayOfYear=7, 7/7 + 1 = 2
        assertEquals(2, Micros.getWeekOfYear(jan7));
    }

    @Test
    void testGetWeekOfYearDec31() {
        long dec31 = Micros.toMicros(2023, false, 12, 31, 0, 0);
        // dayOfYear=365, 365/7 + 1 = 52 + 1 = 53
        assertEquals(53, Micros.getWeekOfYear(dec31));
    }

    // ========================= getIsoYear =========================

    @Test
    void testGetIsoYearMidYear() {
        // In the middle of a year, ISO year == Gregorian year
        long jul1_2023 = Micros.toMicros(2023, false, 7, 1, 0, 0);
        assertEquals(2023, Micros.getIsoYear(jul1_2023));
    }

    @Test
    void testGetIsoYearJan1BelongsToPreviousYear() {
        // 2023-01-01 is Sunday, ISO week 52 of 2022
        long jan1_2023 = Micros.toMicros(2023, false, 1, 1, 0, 0);
        assertEquals(2022, Micros.getIsoYear(jan1_2023));
    }

    @Test
    void testGetIsoYearDec31BelongsToNextYear() {
        // 2024-12-31 is Tuesday, ISO week 1 of 2025
        long dec31_2024 = Micros.toMicros(2024, true, 12, 31, 0, 0);
        assertEquals(2025, Micros.getIsoYear(dec31_2024));
    }

    // ========================= getDayOfTheWeekOfEndOfYear =========================

    @Test
    void testGetDayOfTheWeekOfEndOfYear() {
        // This is used by CommonUtils.getWeeks()
        // 2020 ends on Thursday (Dec 31 2020 is Thursday)
        // The formula: (y + |y/4| - |y/100| + |y/400|) % 7
        // 2020: (2020 + 505 - 20 + 5) % 7 = 2510 % 7 = 4
        assertEquals(4, Micros.getDayOfTheWeekOfEndOfYear(2020));
        // Years with getDayOfTheWeekOfEndOfYear == 4 or previous year == 3 have 53 ISO weeks
        assertEquals(53, CommonUtils.getWeeks(2020));
    }

    @Test
    void testGetDayOfTheWeekOfEndOfYear2023() {
        // 2023 ends on Sunday (Dec 31 2023)
        // (2023 + 505 - 20 + 5) % 7 = 2513 % 7 = 0
        assertEquals(0, Micros.getDayOfTheWeekOfEndOfYear(2023));
    }

    // ========================= addDays =========================

    @Test
    void testAddDaysPositive() {
        assertEquals(Micros.DAY_MICROS, Micros.addDays(0, 1));
        assertEquals(7 * Micros.DAY_MICROS, Micros.addDays(0, 7));
        assertEquals(365 * Micros.DAY_MICROS, Micros.addDays(0, 365));
    }

    @Test
    void testAddDaysNegative() {
        assertEquals(-Micros.DAY_MICROS, Micros.addDays(0, -1));
        assertEquals(-365 * Micros.DAY_MICROS, Micros.addDays(0, -365));
    }

    @Test
    void testAddDaysCrossingMonthBoundary() {
        // Start at Jan 30 2023, add 3 days -> Feb 2 2023
        long jan30 = Micros.toMicros(2023, false, 1, 30, 0, 0);
        long result = Micros.addDays(jan30, 3);
        assertEquals(2023, Micros.getYear(result));
        assertEquals(2, Micros.getMonthOfYear(result, 2023, false));
        assertEquals(2, Micros.getDayOfMonth(result, 2023, 2, false));
    }

    @Test
    void testAddDaysCrossingYearBoundary() {
        // Start at Dec 30 2023, add 5 days -> Jan 4 2024
        long dec30 = Micros.toMicros(2023, false, 12, 30, 0, 0);
        long result = Micros.addDays(dec30, 5);
        assertEquals(2024, Micros.getYear(result));
        boolean leap2024 = CommonUtils.isLeapYear(2024);
        assertEquals(1, Micros.getMonthOfYear(result, 2024, leap2024));
        assertEquals(4, Micros.getDayOfMonth(result, 2024, 1, leap2024));
    }

    @Test
    void testAddDaysPreservesTimeComponent() {
        // Start with a timestamp that has hours/minutes and verify they are preserved
        long start = Micros.toMicros(2023, false, 6, 15, 14, 30);
        long result = Micros.addDays(start, 10);
        assertEquals(14, Micros.getHourOfDay(result));
        assertEquals(30, Micros.getMinuteOfHour(result));
    }

    // ========================= nextOrSameDayOfWeek =========================

    @Test
    void testNextOrSameDayOfWeekSameDay() {
        // 1970-01-01 is Thursday (dow=4). Asking for Thursday should return same.
        assertEquals(0, Micros.nextOrSameDayOfWeek(0, 4));
    }

    @Test
    void testNextOrSameDayOfWeekNextDay() {
        // 1970-01-01 is Thursday (dow=4). Asking for Friday (5) = +1 day
        assertEquals(Micros.DAY_MICROS, Micros.nextOrSameDayOfWeek(0, 5));
    }

    @Test
    void testNextOrSameDayOfWeekWrapAround() {
        // 1970-01-01 is Thursday (dow=4). Asking for Monday (1):
        // thisDow=4 > dow=1, so: 7 - (4 - 1) = 4 days
        assertEquals(4 * Micros.DAY_MICROS, Micros.nextOrSameDayOfWeek(0, 1));
    }

    @Test
    void testNextOrSameDayOfWeekAllDaysFromThursday() {
        // From Thursday (epoch): expected offsets for Mon..Sun
        // Mon=1: +4, Tue=2: +5, Wed=3: +6, Thu=4: +0, Fri=5: +1, Sat=6: +2, Sun=7: +3
        int[] expectedOffsets = {4, 5, 6, 0, 1, 2, 3};
        for (int dow = 1; dow <= 7; dow++) {
            long result = Micros.nextOrSameDayOfWeek(0, dow);
            assertEquals(expectedOffsets[dow - 1] * Micros.DAY_MICROS, result,
                    "nextOrSameDayOfWeek mismatch for dow=" + dow);
        }
    }

    // ========================= previousOrSameDayOfWeek =========================

    @Test
    void testPreviousOrSameDayOfWeekSameDay() {
        // 1970-01-01 is Thursday (dow=4). Asking for Thursday should return same.
        assertEquals(0, Micros.previousOrSameDayOfWeek(0, 4));
    }

    @Test
    void testPreviousOrSameDayOfWeekPreviousDay() {
        // 1970-01-01 is Thursday (dow=4). Asking for Wednesday (3) = -1 day
        assertEquals(-Micros.DAY_MICROS, Micros.previousOrSameDayOfWeek(0, 3));
    }

    @Test
    void testPreviousOrSameDayOfWeekWrapAround() {
        // 1970-01-01 is Thursday (dow=4). Asking for Friday (5):
        // thisDow=4 < dow=5, so: -(7 + (4 - 5)) = -(7 - 1) = -6 days
        assertEquals(-6 * Micros.DAY_MICROS, Micros.previousOrSameDayOfWeek(0, 5));
    }

    @Test
    void testPreviousOrSameDayOfWeekAllDaysFromThursday() {
        // From Thursday (epoch): expected offsets for Mon..Sun
        // Mon=1: -3, Tue=2: -2, Wed=3: -1, Thu=4: 0, Fri=5: -6, Sat=6: -5, Sun=7: -4
        int[] expectedOffsets = {-3, -2, -1, 0, -6, -5, -4};
        for (int dow = 1; dow <= 7; dow++) {
            long result = Micros.previousOrSameDayOfWeek(0, dow);
            assertEquals(expectedOffsets[dow - 1] * Micros.DAY_MICROS, result,
                    "previousOrSameDayOfWeek mismatch for dow=" + dow);
        }
    }

    // ========================= toMicros =========================

    @Test
    void testToMicrosEpoch() {
        long micros = Micros.toMicros(1970, false, 1, 1, 0, 0);
        assertEquals(0, micros);
    }

    @Test
    void testToMicrosRoundTrip() {
        int year = 2023;
        boolean leap = CommonUtils.isLeapYear(year);
        int month = 6;
        int day = 15;
        int hour = 14;
        int minute = 37;

        long micros = Micros.toMicros(year, leap, month, day, hour, minute);
        assertEquals(year, Micros.getYear(micros));
        assertEquals(month, Micros.getMonthOfYear(micros, year, leap));
        assertEquals(day, Micros.getDayOfMonth(micros, year, month, leap));
        assertEquals(hour, Micros.getHourOfDay(micros));
        assertEquals(minute, Micros.getMinuteOfHour(micros));
        assertEquals(0, Micros.getSecondOfMinute(micros));
    }

    @Test
    void testToMicrosLeapYear() {
        int year = 2024;
        boolean leap = true;
        long feb29 = Micros.toMicros(year, leap, 2, 29, 12, 30);
        assertEquals(2024, Micros.getYear(feb29));
        assertEquals(2, Micros.getMonthOfYear(feb29, year, leap));
        assertEquals(29, Micros.getDayOfMonth(feb29, year, 2, leap));
        assertEquals(12, Micros.getHourOfDay(feb29));
        assertEquals(30, Micros.getMinuteOfHour(feb29));
    }

    @Test
    void testToMicrosPreEpoch() {
        int year = 1969;
        boolean leap = CommonUtils.isLeapYear(year);
        long micros = Micros.toMicros(year, leap, 7, 20, 20, 17);
        assertEquals(1969, Micros.getYear(micros));
        assertEquals(7, Micros.getMonthOfYear(micros, year, leap));
        assertEquals(20, Micros.getDayOfMonth(micros, year, 7, leap));
        assertEquals(20, Micros.getHourOfDay(micros));
        assertEquals(17, Micros.getMinuteOfHour(micros));
        assertTrue(micros < 0, "Pre-epoch timestamp should be negative");
    }

    @Test
    void testToMicrosRoundTripMultipleYears() {
        // Test round-trip for a range of dates spanning leap and non-leap years
        int[][] dates = {
                {2000, 2, 29, 23, 59},   // leap year, Feb 29
                {1999, 12, 31, 0, 0},     // end of year
                {2100, 1, 1, 0, 0},       // far future, non-leap century year
                {1960, 6, 15, 10, 30},    // pre-epoch
                {1800, 3, 1, 8, 15},      // far pre-epoch
                {2024, 12, 31, 23, 59},   // end of leap year
        };
        for (int[] d : dates) {
            int y = d[0];
            int m = d[1];
            int day = d[2];
            int h = d[3];
            int mi = d[4];
            boolean leap = CommonUtils.isLeapYear(y);
            long micros = Micros.toMicros(y, leap, m, day, h, mi);
            assertEquals(y, Micros.getYear(micros), "Year mismatch for " + y + "-" + m + "-" + day);
            assertEquals(m, Micros.getMonthOfYear(micros, y, leap), "Month mismatch for " + y + "-" + m + "-" + day);
            assertEquals(day, Micros.getDayOfMonth(micros, y, m, leap), "Day mismatch for " + y + "-" + m + "-" + day);
            assertEquals(h, Micros.getHourOfDay(micros), "Hour mismatch for " + y + "-" + m + "-" + day);
            assertEquals(mi, Micros.getMinuteOfHour(micros), "Minute mismatch for " + y + "-" + m + "-" + day);
        }
    }

    // ========================= yearMicros =========================

    @Test
    void testYearMicrosEpoch() {
        // 1970 start should be 0
        assertEquals(0, Micros.yearMicros(1970, false));
    }

    @Test
    void testYearMicros1971() {
        // 1971 start = 365 days after epoch (1970 is not leap)
        assertEquals(365 * Micros.DAY_MICROS, Micros.yearMicros(1971, false));
    }

    @Test
    void testYearMicros1969() {
        // 1969 start is negative (365 days before epoch, since 1969 is not a leap year)
        assertEquals(-365 * Micros.DAY_MICROS, Micros.yearMicros(1969, false));
    }

    @Test
    void testYearMicrosConsecutiveYears() {
        // Verify that yearMicros advances by the correct number of days for each year
        for (int y = 1970; y < 2030; y++) {
            boolean leap = CommonUtils.isLeapYear(y);
            long start = Micros.yearMicros(y, leap);
            boolean nextLeap = CommonUtils.isLeapYear(y + 1);
            long nextStart = Micros.yearMicros(y + 1, nextLeap);
            long expectedDays = leap ? 366 : 365;
            assertEquals(expectedDays * Micros.DAY_MICROS, nextStart - start,
                    "Year duration mismatch for " + y);
        }
    }

    // ========================= monthOfYearMicros =========================

    @Test
    void testMonthOfYearMicrosJanuary() {
        // January offset is always 0
        assertEquals(0, Micros.monthOfYearMicros(1, false));
        assertEquals(0, Micros.monthOfYearMicros(1, true));
    }

    @Test
    void testMonthOfYearMicrosFebruary() {
        // February offset = 31 days
        assertEquals(31 * Micros.DAY_MICROS, Micros.monthOfYearMicros(2, false));
        assertEquals(31 * Micros.DAY_MICROS, Micros.monthOfYearMicros(2, true));
    }

    @Test
    void testMonthOfYearMicrosMarch() {
        // March offset depends on leap year
        // Non-leap: 31 + 28 = 59 days
        assertEquals(59 * Micros.DAY_MICROS, Micros.monthOfYearMicros(3, false));
        // Leap: 31 + 29 = 60 days
        assertEquals(60 * Micros.DAY_MICROS, Micros.monthOfYearMicros(3, true));
    }

    @Test
    void testMonthOfYearMicrosDecember() {
        // Non-leap: 31+28+31+30+31+30+31+31+30+31+30 = 334 days
        assertEquals(334 * Micros.DAY_MICROS, Micros.monthOfYearMicros(12, false));
        // Leap: 31+29+31+30+31+30+31+31+30+31+30 = 335 days
        assertEquals(335 * Micros.DAY_MICROS, Micros.monthOfYearMicros(12, true));
    }

    // ========================= parseNanosAsMicrosGreedy =========================

    @Test
    void testParseNanosAsMicrosGreedySimple() throws NumericException {
        // "123456789" = 123456789 nanos = 123456 micros (truncated, last 3 digits dropped)
        String s = "123456789";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        int microsValue = Numbers.decodeLowInt(result);
        int len = Numbers.decodeHighInt(result);
        assertEquals(123456, microsValue);
        assertEquals(9, len);
    }

    @Test
    void testParseNanosAsMicrosGreedyShorterThan9Digits() throws NumericException {
        // "123" -> zero-padded to "123000000" nanos = 123000 micros
        String s = "123";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        int microsValue = Numbers.decodeLowInt(result);
        int len = Numbers.decodeHighInt(result);
        assertEquals(123000, microsValue);
        assertEquals(3, len);
    }

    @Test
    void testParseNanosAsMicrosGreedy6Digits() throws NumericException {
        // "123456" -> zero-padded to "123456000" nanos = 123456 micros
        String s = "123456";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        int microsValue = Numbers.decodeLowInt(result);
        int len = Numbers.decodeHighInt(result);
        assertEquals(123456, microsValue);
        assertEquals(6, len);
    }

    @Test
    void testParseNanosAsMicrosGreedyNegative1Digit() throws NumericException {
        // "-1" -> len=2, padding to 9 positions (including sign), val=-1 padded to -10000000, /1000 = -10000
        String s = "-1";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        assertEquals(-10000, Numbers.decodeLowInt(result));
        assertEquals(2, Numbers.decodeHighInt(result));
    }

    @Test
    void testParseNanosAsMicrosGreedyNegative3Digits() throws NumericException {
        // "-123" -> len=4, val=-123 padded to -12300000, /1000 = -12300
        String s = "-123";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        assertEquals(-12300, Numbers.decodeLowInt(result));
        assertEquals(4, Numbers.decodeHighInt(result));
    }

    @Test
    void testParseNanosAsMicrosGreedyNegative6Digits() throws NumericException {
        // "-123456" -> len=7, val=-123456 padded to -12345600, /1000 = -12345
        String s = "-123456";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        assertEquals(-12345, Numbers.decodeLowInt(result));
        assertEquals(7, Numbers.decodeHighInt(result));
    }

    @Test
    void testParseNanosAsMicrosGreedyNegative8Digits() throws NumericException {
        // "-12345678" -> len=9, no padding needed, val=-12345678, /1000 = -12345
        String s = "-12345678";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        assertEquals(-12345, Numbers.decodeLowInt(result));
        assertEquals(9, Numbers.decodeHighInt(result));
    }

    @Test
    void testParseNanosAsMicrosGreedyNegative9DigitsThrows() {
        // "-123456789" has len=10 which exceeds the max of 9, so it throws
        String s = "-123456789";
        assertThrows(NumericException.class, () -> Micros.parseNanosAsMicrosGreedy(s, 0, s.length()));
    }

    @Test
    void testParseNanosAsMicrosGreedySingleDigit() throws NumericException {
        // "5" -> zero-padded to "500000000" nanos = 500000 micros (but internal accumulation
        // uses negation, then final result is negated back for positive input)
        String s = "5";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        int microsValue = Numbers.decodeLowInt(result);
        assertEquals(500000, microsValue);
        assertEquals(1, Numbers.decodeHighInt(result));
    }

    @Test
    void testParseNanosAsMicrosGreedyStopsAtNonDigit() throws NumericException {
        // "123Z" -> "123" parsed, stopped at 'Z', padded to "123000000" nanos = 123000
        String s = "123Z";
        long result = Micros.parseNanosAsMicrosGreedy(s, 0, s.length());
        int microsValue = Numbers.decodeLowInt(result);
        int len = Numbers.decodeHighInt(result);
        assertEquals(123000, microsValue);
        assertEquals(3, len);
    }

    @Test
    void testParseNanosAsMicrosGreedyEmpty() {
        String s = "";
        assertThrows(NumericException.class, () -> Micros.parseNanosAsMicrosGreedy(s, 0, s.length()));
    }

    @Test
    void testParseNanosAsMicrosGreedyTooManyDigits() {
        // More than 9 digits should throw
        String s = "1234567890";
        assertThrows(NumericException.class, () -> Micros.parseNanosAsMicrosGreedy(s, 0, s.length()));
    }

    @Test
    void testParseNanosAsMicrosGreedyOffset() throws NumericException {
        // Parse starting from an offset within the string
        String s = "xxx123456789";
        long result = Micros.parseNanosAsMicrosGreedy(s, 3, s.length());
        int microsValue = Numbers.decodeLowInt(result);
        assertEquals(123456, microsValue);
    }

    @Test
    void testParseNanosAsMicrosGreedyOnlyNonDigit() {
        String s = "Z";
        assertThrows(NumericException.class, () -> Micros.parseNanosAsMicrosGreedy(s, 0, s.length()));
    }

    @Test
    void testParseNanosAsMicrosGreedyNegativeOnlySign() {
        String s = "-";
        assertThrows(NumericException.class, () -> Micros.parseNanosAsMicrosGreedy(s, 0, s.length()));
    }

    // ========================= Epoch decomposition (comprehensive) =========================

    @Test
    void testEpochComponentsFull() {
        // epoch = 0 should be 1970-01-01 00:00:00.000000
        assertEquals(1970, Micros.getYear(0));
        assertEquals(1, Micros.getMonthOfYear(0, 1970, false));
        assertEquals(1, Micros.getDayOfMonth(0, 1970, 1, false));
        assertEquals(0, Micros.getHourOfDay(0));
        assertEquals(0, Micros.getMinuteOfHour(0));
        assertEquals(0, Micros.getSecondOfMinute(0));
        assertEquals(0, Micros.getMillisOfSecond(0));
        assertEquals(0, Micros.getMicrosOfMilli(0));
        assertEquals(0, Micros.getMicrosOfSecond(0));
        assertEquals(4, Micros.getDayOfWeek(0));     // Thursday
        assertEquals(5, Micros.getDayOfWeekSundayFirst(0)); // Thursday in SundayFirst
        assertEquals(1, Micros.getDayOfYear(0));     // Jan 1
    }

    // ========================= Known timestamp decomposition =========================

    @Test
    void testKnownTimestamp2024Feb29Noon() {
        // 2024-02-29 12:30:45.123456
        int year = 2024;
        boolean leap = true;
        long micros = Micros.toMicros(year, leap, 2, 29, 12, 30);
        // Add seconds and microseconds manually
        micros += 45 * Micros.SECOND_MICROS + 123 * Micros.MILLI_MICROS + 456;

        assertEquals(2024, Micros.getYear(micros));
        assertEquals(2, Micros.getMonthOfYear(micros, year, leap));
        assertEquals(29, Micros.getDayOfMonth(micros, year, 2, leap));
        assertEquals(12, Micros.getHourOfDay(micros));
        assertEquals(30, Micros.getMinuteOfHour(micros));
        assertEquals(45, Micros.getSecondOfMinute(micros));
        assertEquals(123, Micros.getMillisOfSecond(micros));
        assertEquals(456, Micros.getMicrosOfMilli(micros));
        assertEquals(123456, Micros.getMicrosOfSecond(micros));
    }

    @Test
    void testKnownTimestampPreEpoch1969Dec31() {
        // 1969-12-31 23:59:59.999999 = epoch - 1 microsecond
        long micros = -1;
        assertEquals(1969, Micros.getYear(micros));
        assertEquals(23, Micros.getHourOfDay(micros));
        assertEquals(59, Micros.getMinuteOfHour(micros));
        assertEquals(59, Micros.getSecondOfMinute(micros));
        assertEquals(999, Micros.getMillisOfSecond(micros));
        assertEquals(999, Micros.getMicrosOfMilli(micros));
        assertEquals(999999, Micros.getMicrosOfSecond(micros));
    }

    @Test
    void testKnownTimestampPreEpoch1888() {
        int year = 1888;
        boolean leap = CommonUtils.isLeapYear(year);
        assertTrue(leap); // 1888 is a leap year
        long micros = Micros.toMicros(year, leap, 5, 12, 23, 45);
        assertEquals(1888, Micros.getYear(micros));
        assertEquals(5, Micros.getMonthOfYear(micros, year, leap));
        assertEquals(12, Micros.getDayOfMonth(micros, year, 5, leap));
        assertEquals(23, Micros.getHourOfDay(micros));
        assertEquals(45, Micros.getMinuteOfHour(micros));
    }

    // ========================= Negative timestamp edge cases =========================

    @Test
    void testNegativeTimestampExactDayBoundary() {
        // Exactly 1 day before epoch
        long micros = -Micros.DAY_MICROS;
        assertEquals(1969, Micros.getYear(micros));
        assertEquals(0, Micros.getHourOfDay(micros));
        assertEquals(0, Micros.getMinuteOfHour(micros));
        assertEquals(0, Micros.getSecondOfMinute(micros));
        assertEquals(0, Micros.getMillisOfSecond(micros));
        assertEquals(0, Micros.getMicrosOfMilli(micros));
    }

    @Test
    void testNegativeTimestampExactHourBoundary() {
        // Exactly 1 hour before epoch = 23:00:00 on Dec 31 1969
        long micros = -Micros.HOUR_MICROS;
        assertEquals(23, Micros.getHourOfDay(micros));
        assertEquals(0, Micros.getMinuteOfHour(micros));
        assertEquals(0, Micros.getSecondOfMinute(micros));
    }

    @Test
    void testNegativeTimestampExactMinuteBoundary() {
        // Exactly 1 minute before epoch = 23:59:00
        long micros = -Micros.MINUTE_MICROS;
        assertEquals(23, Micros.getHourOfDay(micros));
        assertEquals(59, Micros.getMinuteOfHour(micros));
        assertEquals(0, Micros.getSecondOfMinute(micros));
    }

    @Test
    void testNegativeTimestampExactSecondBoundary() {
        // Exactly 1 second before epoch = 23:59:59.000000
        long micros = -Micros.SECOND_MICROS;
        assertEquals(23, Micros.getHourOfDay(micros));
        assertEquals(59, Micros.getMinuteOfHour(micros));
        assertEquals(59, Micros.getSecondOfMinute(micros));
        assertEquals(0, Micros.getMillisOfSecond(micros));
        assertEquals(0, Micros.getMicrosOfMilli(micros));
    }

    // ========================= Complete round-trip stress test =========================

    @Test
    void testComprehensiveRoundTrip() {
        // Test round-trip decomposition for many known dates
        int[][] testDates = {
                // year, month, day, hour, minute
                {1970, 1, 1, 0, 0},
                {2000, 1, 1, 0, 0},
                {2000, 2, 29, 12, 0},
                {1999, 12, 31, 23, 59},
                {2023, 6, 15, 14, 37},
                {2024, 2, 29, 0, 0},
                {2024, 3, 1, 0, 0},
                {1969, 12, 31, 23, 59},
                {1969, 1, 1, 0, 0},
                {1900, 1, 1, 0, 0},
                {1900, 12, 31, 23, 59},
                {2100, 1, 1, 12, 30},
        };

        for (int[] d : testDates) {
            int y = d[0], m = d[1], day = d[2], h = d[3], mi = d[4];
            boolean leap = CommonUtils.isLeapYear(y);
            long micros = Micros.toMicros(y, leap, m, day, h, mi);

            String label = y + "-" + m + "-" + day + "T" + h + ":" + mi;
            assertEquals(y, Micros.getYear(micros), "Year: " + label);
            assertEquals(m, Micros.getMonthOfYear(micros, y, leap), "Month: " + label);
            assertEquals(day, Micros.getDayOfMonth(micros, y, m, leap), "Day: " + label);
            assertEquals(h, Micros.getHourOfDay(micros), "Hour: " + label);
            assertEquals(mi, Micros.getMinuteOfHour(micros), "Minute: " + label);
            assertEquals(0, Micros.getSecondOfMinute(micros), "Second: " + label);
        }
    }
}
