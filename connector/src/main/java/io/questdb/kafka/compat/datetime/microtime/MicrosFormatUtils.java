/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.kafka.compat.datetime.microtime;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Os;
import io.questdb.kafka.compat.datetime.CommonUtils;
import io.questdb.kafka.compat.datetime.DateLocale;
import io.questdb.kafka.compat.datetime.millitime.DateFormatUtils;
import io.questdb.client.std.str.CharSink;

import static io.questdb.kafka.compat.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public class MicrosFormatUtils {
    static int thisCenturyLow;

    static {
        long micros = Os.currentTimeMicros();
        int year = Micros.getYear(micros);
        int centuryOffset = year % 100;
        int thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLow = year - centuryOffset + 100;
        } else {
            thisCenturyLow = year - centuryOffset;
        }
    }

    public static void updateReferenceYear(long micros) {
        int year = Micros.getYear(micros);
        int centuryOffset = year % 100;
        int thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLow = year - centuryOffset + 100;
        } else {
            thisCenturyLow = year - centuryOffset;
        }
    }

    public static int adjustYear(int year) {
        return thisCenturyLow + year;
    }

    public static void append0(CharSink<?> sink, int val) {
        DateFormatUtils.append0(sink, val);
    }

    public static void append00(CharSink<?> sink, int val) {
        DateFormatUtils.append00(sink, val);
    }

    public static void append00000(CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0').putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0').putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 1000) {
            sink.putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 10000) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 100000) {
            sink.putAscii('0');
        }
        sink.put(val);
    }

    public static void appendAmPm(CharSink<?> sink, int hour, DateLocale locale) {
        if (hour < 12) {
            sink.putAscii(locale.getAMPM(0));
        } else {
            sink.putAscii(locale.getAMPM(1));
        }
    }

    public static void appendEra(CharSink<?> sink, int year, DateLocale locale) {
        if (year < 0) {
            sink.put(locale.getEra(0));
        } else {
            sink.put(locale.getEra(1));
        }
    }

    public static void appendHour12(CharSink<?> sink, int hour) {
        Numbers.append(sink, hour % 12);
    }

    public static void appendHour121(CharSink<?> sink, int hour) {
        DateFormatUtils.appendHour121(sink, hour);
    }

    public static void appendHour121Padded(CharSink<?> sink, int hour) {
        int h12 = (hour + 11) % 12 + 1;
        append0(sink, h12);
    }

    public static void appendHour12Padded(CharSink<?> sink, int hour) {
        append0(sink, hour % 12);
    }

    public static void appendHour241(CharSink<?> sink, int hour) {
        DateFormatUtils.appendHour241(sink, hour);
    }

    public static void appendHour241Padded(CharSink<?> sink, int hour) {
        int h24 = (hour + 23) % 24 + 1;
        append0(sink, h24);
    }

    public static void appendYear(CharSink<?> sink, int val) {
        Numbers.append(sink, val != 0 ? val : 1);
    }

    public static void appendYear0(CharSink<?> sink, int val) {
        if (Math.abs(val) < 10) {
            sink.putAscii('0');
        }
        appendYear(sink, val);
    }

    public static void appendYear00(CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0');
        }
        appendYear(sink, val);
    }

    public static void appendYear000(CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 1000) {
            sink.putAscii('0');
        }
        appendYear(sink, val);
    }

    public static void assertChar(char c, CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos, hi);
        if (in.charAt(pos) != c) {
            throw NumericException.instance();
        }
    }

    public static void assertNoTail(int pos, int hi) throws NumericException {
        if (pos < hi) {
            throw NumericException.instance();
        }
    }

    public static void assertRemaining(int pos, int hi) throws NumericException {
        DateFormatUtils.assertRemaining(pos, hi);
    }

    public static int assertString(CharSequence delimiter, int len, CharSequence in, int pos, int hi) throws NumericException {
        return DateFormatUtils.assertString(delimiter, len, in, pos, hi);
    }

    public static long compute(
            DateLocale locale,
            int era,
            int year,
            int month,
            int week,
            int day,
            int hour,
            int minute,
            int second,
            int millis,
            int micros,
            int timezone,
            long offsetMinutes,
            int hourType
    ) throws NumericException {
        if (era == 0) {
            year = -(year - 1);
        }

        boolean leap = CommonUtils.isLeapYear(year);

        // wrong month
        if (month < 1 || month > 12) {
            throw NumericException.instance();
        }

        if (hourType == CommonUtils.HOUR_24) {
            // wrong 24-hour clock hour
            if (hour < 0 || hour > 24) {
                throw NumericException.instance();
            }
            hour %= 24;
        } else {
            // wrong 12-hour clock hour
            if (hour < 0 || hour > 12) {
                throw NumericException.instance();
            }
            hour %= 12;
            if (hourType == CommonUtils.HOUR_PM) {
                hour += 12;
            }
        }

        // wrong day of month
        if (day < 1 || day > CommonUtils.getDaysPerMonth(month, leap)) {
            throw NumericException.instance();
        }

        if (minute < 0 || minute > 59) {
            throw NumericException.instance();
        }

        if (second < 0 || second > 59) {
            throw NumericException.instance();
        }

        if ((week <= 0 && week != -1) || week > CommonUtils.getWeeks(year)) {
            throw NumericException.instance();
        }

        // calculate year, month, and day of ISO week
        if (week != -1) {
            long firstDayOfIsoWeekMicros = Micros.yearMicros(year, CommonUtils.isLeapYear(year)) +
                    (week - 1) * Micros.WEEK_MICROS +
                    CommonUtils.getIsoYearDayOffset(year) * Micros.DAY_MICROS;
            month = Micros.getMonthOfYear(firstDayOfIsoWeekMicros);
            year += (week == 1 && CommonUtils.getIsoYearDayOffset(year) < 0) ? -1 : 0;
            day = Micros.getDayOfMonth(firstDayOfIsoWeekMicros, year, month, CommonUtils.isLeapYear(year));
        }

        long outMicros = Micros.yearMicros(year, leap)
                + Micros.monthOfYearMicros(month, leap)
                + (long) (day - 1) * Micros.DAY_MICROS
                + (long) hour * Micros.HOUR_MICROS
                + (long) minute * Micros.MINUTE_MICROS
                + (long) second * Micros.SECOND_MICROS
                + (long) millis * Micros.MILLI_MICROS
                + micros;

        if (timezone > -1) {
            outMicros -= locale.getZoneRules(timezone, RESOLUTION_MICROS).getOffset(outMicros, year);
        } else if (offsetMinutes > Long.MIN_VALUE) {
            outMicros -= offsetMinutes * Micros.MINUTE_MICROS;
        }

        return outMicros;
    }

    public static long parseOptionalMicrosGreedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (isOptionalFractionStart(sequence, p, lim)) {
            final long parsed = Numbers.parseLong000000Greedy(sequence, p + 1, lim);
            return Numbers.encodeLowHighInts(Numbers.decodeLowInt(parsed), Numbers.decodeHighInt(parsed) + 1);
        }
        return Numbers.encodeLowHighInts(0, 0);
    }

    public static long parseOptionalNanosAsMicrosGreedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (isOptionalFractionStart(sequence, p, lim)) {
            final long parsed = Micros.parseNanosAsMicrosGreedy(sequence, p + 1, lim);
            return Numbers.encodeLowHighInts(Numbers.decodeLowInt(parsed), Numbers.decodeHighInt(parsed) + 1);
        }
        return Numbers.encodeLowHighInts(0, 0);
    }

    private static boolean isOptionalFractionStart(CharSequence sequence, int p, int lim) {
        if (p + 1 >= lim || sequence.charAt(p) != '.') {
            return false;
        }
        final char next = sequence.charAt(p + 1);
        return next >= '0' && next <= '9';
    }

    public static long parseYearGreedy(CharSequence in, int pos, int hi) throws NumericException {
        long l = Numbers.parseIntSafely(in, pos, hi);
        int len = Numbers.decodeHighInt(l);
        int year;
        if (len == 2) {
            year = adjustYear(Numbers.decodeLowInt(l));
        } else {
            year = Numbers.decodeLowInt(l);
        }
        return Numbers.encodeLowHighInts(year, len);
    }
}
