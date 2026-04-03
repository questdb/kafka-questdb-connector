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

package io.questdb.kafka.compat.datetime;

import io.questdb.kafka.compat.datetime.microtime.Micros;

public class CommonUtils {
    public static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    public static final int DAY_HOURS = 24;
    public static final int HOUR_24 = 2;
    public static final int HOUR_AM = 0;
    public static final int HOUR_MINUTES = 60;
    public static final int HOUR_PM = 1;
    public static final String USEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUz";
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";

    /**
     * Days in a given month. This method expects you to know if month is in leap year.
     *
     * @param m    month from 1 to 12
     * @param leap true if this is for leap year
     * @return number of days in month.
     */
    public static int getDaysPerMonth(int m, boolean leap) {
        return leap & m == 2 ? 29 : DAYS_PER_MONTH[m - 1];
    }

    /**
     * Since ISO weeks don't always start on the first day of the year, there is an offset of days from the 1st day of the year.
     *
     * @param year of timestamp
     * @return difference in the days from the start of the year (January 1st) and the first ISO week
     */
    public static int getIsoYearDayOffset(int year) {
        int dayOfTheWeekOfEndOfPreviousYear = Micros.getDayOfTheWeekOfEndOfYear(year - 1);
        return ((dayOfTheWeekOfEndOfPreviousYear <= 3) ? 0 : 7) - dayOfTheWeekOfEndOfPreviousYear;
    }

    public static int getWeeks(int y) {
        if (Micros.getDayOfTheWeekOfEndOfYear(y) == 4 || Micros.getDayOfTheWeekOfEndOfYear(y - 1) == 3) {
            return 53;
        }
        return 52;
    }

    /**
     * Calculates if year is leap year using following algorithm:
     * <p>
     * <a href="http://en.wikipedia.org/wiki/Leap_year">...</a>
     *
     * @param year the year
     * @return true if year is leap
     */
    public static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }
}
