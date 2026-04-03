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
import io.questdb.kafka.compat.datetime.CommonUtils;

import static io.questdb.kafka.compat.datetime.CommonUtils.DAYS_PER_MONTH;

public final class Micros {
    public static final long DAY_MICROS = 86_400_000_000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long AVG_YEAR_MICROS = (long) (365.2425 * DAY_MICROS);
    public static final long HOUR_MICROS = 3600000000L;
    public static final long MICRO_NANOS = 1000L;
    public static final long MILLI_MICROS = 1000L;
    public static final long MINUTE_MICROS = 60000000L;
    public static final long MINUTE_SECONDS = 60L;
    public static final long SECOND_MICROS = 1000000L;
    public static final int SECOND_MILLIS = 1000;
    public static final long WEEK_MICROS = 7 * DAY_MICROS;
    public static final long YEAR_MICROS_NONLEAP = 365 * DAY_MICROS;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long[] MAX_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long[] MIN_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long YEAR_MICROS_LEAP = 366 * DAY_MICROS;

    private Micros() {
    }

    public static long addDays(long micros, int days) {
        return micros + days * DAY_MICROS;
    }

    public static int getDayOfMonth(long micros, int year, int month, boolean leap) {
        long yearMicros = yearMicros(year, leap);
        yearMicros += monthOfYearMicros(month, leap);
        return (int) ((micros - yearMicros) / DAY_MICROS) + 1;
    }

    public static int getDayOfTheWeekOfEndOfYear(int year) {
        return (year + Math.abs(year / 4) - Math.abs(year / 100) + Math.abs(year / 400)) % 7;
    }

    public static int getDayOfWeek(long micros) {
        // 1970-01-01 is Thursday.
        long d;
        if (micros > -1) {
            d = micros / DAY_MICROS;
        } else {
            d = (micros - (DAY_MICROS - 1)) / DAY_MICROS;
            if (d < -3) {
                return 7 + (int) ((d + 4) % 7);
            }
        }
        return 1 + (int) ((d + 3) % 7);
    }

    public static int getDayOfWeekSundayFirst(long micros) {
        // 1970-01-01 is Thursday.
        long d;
        if (micros > -1) {
            d = micros / DAY_MICROS;
        } else {
            d = (micros - (DAY_MICROS - 1)) / DAY_MICROS;
            if (d < -4) {
                return 7 + (int) ((d + 5) % 7);
            }
        }
        return 1 + (int) ((d + 4) % 7);
    }

    public static int getDayOfYear(long micros) {
        int year = getYear(micros);
        boolean leap = CommonUtils.isLeapYear(year);
        long yearStart = yearMicros(year, leap);
        return (int) ((micros - yearStart) / DAY_MICROS) + 1;
    }

    public static int getDoy(long micros) {
        final int year = getYear(micros);
        final boolean leap = CommonUtils.isLeapYear(year);
        final long yearStart = yearMicros(year, leap);
        return (int) ((micros - yearStart) / DAY_MICROS) + 1;
    }

    public static int getHourOfDay(long micros) {
        if (micros > -1) {
            return (int) ((micros / HOUR_MICROS) % CommonUtils.DAY_HOURS);
        } else {
            return CommonUtils.DAY_HOURS - 1 + (int) (((micros + 1) / HOUR_MICROS) % CommonUtils.DAY_HOURS);
        }
    }

    // Each ISO 8601 week-numbering year begins with the Monday of the week containing the 4th of January,
    // so in early January or late December the ISO year may be different from the Gregorian year.
    // See the getWeek() method for more information.
    public static int getIsoYear(long micros) {
        int w = (10 + getDoy(micros) - getDayOfWeek(micros)) / 7;
        int y = getYear(micros);
        if (w < 1) {
            return y - 1;
        }

        if (w > CommonUtils.getWeeks(y)) {
            return y + 1;
        }

        return y;
    }

    public static int getMicrosOfMilli(long micros) {
        if (micros > -1) {
            return (int) (micros % MILLI_MICROS);
        } else {
            return (int) (MILLI_MICROS - 1 + ((micros + 1) % MILLI_MICROS));
        }
    }

    public static int getMicrosOfSecond(long micros) {
        if (micros > -1) {
            return (int) (micros % SECOND_MICROS);
        } else {
            return (int) (SECOND_MICROS - 1 + ((micros + 1) % SECOND_MICROS));
        }
    }

    public static int getMillisOfSecond(long micros) {
        if (micros > -1) {
            return (int) ((micros / MILLI_MICROS) % SECOND_MILLIS);
        } else {
            return SECOND_MILLIS - 1 + (int) (((micros + 1) / MILLI_MICROS) % SECOND_MILLIS);
        }
    }

    public static int getMinuteOfHour(long micros) {
        if (micros > -1) {
            return (int) ((micros / MINUTE_MICROS) % CommonUtils.HOUR_MINUTES);
        } else {
            return CommonUtils.HOUR_MINUTES - 1 + (int) (((micros + 1) / MINUTE_MICROS) % CommonUtils.HOUR_MINUTES);
        }
    }

    public static int getMonthOfYear(long micros) {
        final int y = Micros.getYear(micros);
        final boolean leap = CommonUtils.isLeapYear(y);
        return getMonthOfYear(micros, y, leap);
    }

    /**
     * Calculates month of year from absolute micros.
     *
     * @param micros micros since 1970
     * @param year   year of month
     * @param leap   true if year was leap
     * @return month of year
     */
    public static int getMonthOfYear(long micros, int year, boolean leap) {
        int i = (int) (((micros - yearMicros(year, leap)) / 1000) >> 10);
        return leap
                ? ((i < 182 * 84375)
                ? ((i < 91 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 60 * 84375) ? 2 : 3)
                : ((i < 121 * 84375) ? 4 : (i < 152 * 84375) ? 5 : 6))
                : ((i < 274 * 84375)
                ? ((i < 213 * 84375) ? 7 : (i < 244 * 84375) ? 8 : 9)
                : ((i < 305 * 84375) ? 10 : (i < 335 * 84375) ? 11 : 12)))
                : ((i < 181 * 84375)
                ? ((i < 90 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 59 * 84375) ? 2 : 3)
                : ((i < 120 * 84375) ? 4 : (i < 151 * 84375) ? 5 : 6))
                : ((i < 273 * 84375)
                ? ((i < 212 * 84375) ? 7 : (i < 243 * 84375) ? 8 : 9)
                : ((i < 304 * 84375) ? 10 : (i < 334 * 84375) ? 11 : 12)));
    }

    public static int getSecondOfMinute(long micros) {
        if (micros > -1) {
            return (int) ((micros / SECOND_MICROS) % MINUTE_SECONDS);
        } else {
            return (int) (MINUTE_SECONDS - 1 + (int) (((micros + 1) / SECOND_MICROS) % MINUTE_SECONDS));
        }
    }

    // https://en.wikipedia.org/wiki/ISO_week_date
    public static int getWeek(long micros) {
        int w = (10 + getDoy(micros) - getDayOfWeek(micros)) / 7;
        int y = getYear(micros);
        if (w < 1) {
            return CommonUtils.getWeeks(y - 1);
        }

        if (w > CommonUtils.getWeeks(y)) {
            return 1;
        }

        return w;
    }

    public static int getWeekOfYear(long micros) {
        return getDayOfYear(micros) / 7 + 1;
    }

    public static int getYear(long micros) {
        // Initial year estimate relative to 1970
        // Use a reasonable approximation of days per year to avoid overflow
        // 365.25 days per year approximation
        int yearsSinceEpoch = (int) (micros / AVG_YEAR_MICROS);
        int yearEstimate = 1970 + yearsSinceEpoch;

        // Handle negative years appropriately
        if (micros < 0 && yearEstimate >= 1970) {
            yearEstimate = 1969;
        }

        // Calculate year start
        boolean leap = CommonUtils.isLeapYear(yearEstimate);
        long yearStart = yearMicros(yearEstimate, leap);

        // Check if we need to adjust
        long diff = micros - yearStart;

        if (diff < 0) {
            // We're in the previous year
            yearEstimate--;
        } else {
            // Check if we're in the next year
            long yearLength = leap ? YEAR_MICROS_LEAP : YEAR_MICROS_NONLEAP;
            if (diff >= yearLength) {
                yearEstimate++;
            }
        }

        return yearEstimate;
    }

    public static long monthOfYearMicros(int month, boolean leap) {
        return leap ? MAX_MONTH_OF_YEAR_MICROS[month - 1] : MIN_MONTH_OF_YEAR_MICROS[month - 1];
    }

    public static long nextOrSameDayOfWeek(long millis, int dow) {
        int thisDow = getDayOfWeek(millis);
        if (thisDow == dow) {
            return millis;
        }

        if (thisDow < dow) {
            return millis + (dow - thisDow) * DAY_MICROS;
        } else {
            return millis + (7 - (thisDow - dow)) * DAY_MICROS;
        }
    }

    public static long parseNanosAsMicrosGreedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance();
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || Numbers.notDigit(sequence.charAt(i))) {
            throw NumericException.instance();
        }

        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);

            if (Numbers.notDigit(c)) {
                break;
            }

            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.instance();
            }
            val = r;
        }

        final int len = i - p;

        if (len > 9 || val == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance();
        }

        while (i - p < 9) {
            val *= 10;
            i++;
        }

        val /= 1000;

        return Numbers.encodeLowHighInts(negative ? val : -val, len);
    }

    public static long previousOrSameDayOfWeek(long micros, int dow) {
        int thisDow = getDayOfWeek(micros);
        if (thisDow == dow) {
            return micros;
        }

        if (thisDow < dow) {
            return micros - (7 + (thisDow - dow)) * DAY_MICROS;
        } else {
            return micros - (thisDow - dow) * DAY_MICROS;
        }
    }

    public static long toMicros(int y, boolean leap, int m, int d, int h, int mi) {
        return yearMicros(y, leap) + monthOfYearMicros(m, leap) + (d - 1) * DAY_MICROS + h * HOUR_MICROS + mi * MINUTE_MICROS;
    }

    /**
     * Calculated epoch offset in microseconds of the beginning of the year. For example of year 2008 this is
     * equivalent to parsing "2008-01-01T00:00:00.000Z", except this method is faster.
     *
     * @param year the year
     * @param leap true if given year is leap year
     * @return epoch offset in micros.
     */
    public static long yearMicros(int year, boolean leap) {
        int leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (leap) {
                leapYears--;
            }
        }

        long days = year * 365L + (leapYears - DAYS_0000_TO_1970);
        long micros = days * DAY_MICROS;
        if (days < 0 & micros > 0) {
            return Long.MIN_VALUE;
        }
        return micros;
    }

    static {
        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            minSum += DAYS_PER_MONTH[i] * DAY_MICROS;
            MIN_MONTH_OF_YEAR_MICROS[i + 1] = minSum;
            maxSum += CommonUtils.getDaysPerMonth(i + 1, true) * DAY_MICROS;
            MAX_MONTH_OF_YEAR_MICROS[i + 1] = maxSum;
        }
    }
}
