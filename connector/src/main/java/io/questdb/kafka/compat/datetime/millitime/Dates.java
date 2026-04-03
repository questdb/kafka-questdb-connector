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

package io.questdb.kafka.compat.datetime.millitime;

import io.questdb.client.std.Chars;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.kafka.compat.datetime.DateLocale;
import io.questdb.kafka.compat.datetime.TimeZoneRules;

import static io.questdb.kafka.compat.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public final class Dates {

    public static final long DAY_MILLIS = 86400000L;
    public static final long HOUR_MILLIS = 3600000L;
    public static final long MINUTE_MILLIS = 60000L;
    public static final long SECOND_MILLIS = 1000L;
    public static final int STATE_DELIM = 4;
    public static final int STATE_END = 6;
    public static final int STATE_GMT = 2;
    public static final int STATE_HOUR = 3;
    public static final int STATE_INIT = 0;
    public static final int STATE_MINUTE = 5;
    public static final int STATE_SIGN = 7;
    public static final int STATE_UTC = 1;
    private static final char AFTER_NINE = '9' + 1;
    private static final char BEFORE_ZERO = '0' - 1;
    private static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };

    private Dates() {
    }

    public static long parseOffset(CharSequence in) {
        return parseOffset(in, 0, in.length());
    }

    public static long parseOffset(CharSequence in, int lo, int hi) {
        int p = lo;
        int state = STATE_INIT;
        boolean negative = false;
        int hour = 0;
        int minute = 0;

        try {
            OUT:
            while (p < hi) {
                char c = in.charAt(p);
                switch (state) {
                    case STATE_INIT:
                        switch (c) {
                            case 'U':
                            case 'u':
                                state = STATE_UTC;
                                break;
                            case 'G':
                            case 'g':
                                state = STATE_GMT;
                                break;
                            case 'Z':
                            case 'z':
                                state = STATE_END;
                                break;
                            case '+':
                                state = STATE_HOUR;
                                break;
                            case '-':
                                negative = true;
                                state = STATE_HOUR;
                                break;
                            default:
                                if (isDigit(c)) {
                                    state = STATE_HOUR;
                                    p--;
                                } else {
                                    return Long.MIN_VALUE;
                                }
                                break;
                        }
                        p++;
                        break;
                    case STATE_UTC:
                        if (p > hi - 2 || Chars.noMatch(in, p, p + 2, "tc", 0, 2)) {
                            return Long.MIN_VALUE;
                        }
                        state = STATE_SIGN;
                        p += 2;
                        break;
                    case STATE_GMT:
                        if (p > hi - 2 || Chars.noMatch(in, p, p + 2, "mt", 0, 2)) {
                            return Long.MIN_VALUE;
                        }
                        state = STATE_SIGN;
                        p += 2;
                        break;
                    case STATE_SIGN:
                        switch (c) {
                            case '+':
                                break;
                            case '-':
                                negative = true;
                                break;
                            default:
                                return Long.MIN_VALUE;
                        }
                        p++;
                        state = STATE_HOUR;
                        break;
                    case STATE_HOUR:
                        if (isDigit(c) && p < hi - 1) {
                            hour = Numbers.parseInt(in, p, p + 2);
                        } else {
                            return Long.MIN_VALUE;
                        }
                        state = STATE_DELIM;
                        p += 2;
                        break;
                    case STATE_DELIM:
                        if (c == ':') {
                            state = STATE_MINUTE;
                            p++;
                        } else if (isDigit(c)) {
                            state = STATE_MINUTE;
                        } else {
                            return Long.MIN_VALUE;
                        }
                        break;
                    case STATE_MINUTE:
                        if (isDigit(c) && p < hi - 1) {
                            minute = Numbers.parseInt(in, p, p + 2);
                        } else {
                            return Long.MIN_VALUE;
                        }
                        p += 2;
                        state = STATE_END;
                        break OUT;
                    default:
                        return Long.MIN_VALUE;
                }
            }
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }

        switch (state) {
            case STATE_DELIM:
            case STATE_END:
                if (hour > 23 || minute > 59) {
                    return Long.MIN_VALUE;
                }
                final int min = hour * 60 + minute;
                return Numbers.encodeLowHighInts(negative ? -min : min, p - lo);
            default:
                return Long.MIN_VALUE;
        }
    }

    private static boolean isDigit(char c) {
        return c > BEFORE_ZERO && c < AFTER_NINE;
    }
}
