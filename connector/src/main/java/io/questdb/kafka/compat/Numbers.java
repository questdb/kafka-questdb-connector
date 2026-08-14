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

package io.questdb.kafka.compat;

import io.questdb.client.std.NumericException;
import io.questdb.client.std.str.CharSink;

public final class Numbers {
    private Numbers() {
    }

    public static void append(CharSink<?> sink, int value) {
        io.questdb.client.std.Numbers.append(sink, value);
    }

    public static void append(CharSink<?> sink, long value) {
        io.questdb.client.std.Numbers.append(sink, value);
    }

    public static int decodeHighInt(long value) {
        return io.questdb.client.std.Numbers.decodeHighInt(value);
    }

    public static int decodeLowInt(long value) {
        return io.questdb.client.std.Numbers.decodeLowInt(value);
    }

    public static long encodeLowHighInts(int low, int high) {
        return io.questdb.client.std.Numbers.encodeLowHighInts(low, high);
    }

    public static boolean notDigit(char value) {
        return io.questdb.client.std.Numbers.notDigit(value);
    }

    public static int parseInt(CharSequence sequence, int lo, int hi) throws NumericException {
        return io.questdb.client.std.Numbers.parseInt(sequence, lo, hi);
    }

    public static long parseInt000Greedy(CharSequence sequence, int lo, int hi) throws NumericException {
        long parsed = parseIntSafely(sequence, lo, hi);
        int length = decodeHighInt(parsed);
        int value = decodeLowInt(parsed);
        if (length > 3) {
            throw NumericException.instance().put("number overflow");
        }
        for (int i = length; i < 3; i++) {
            value *= 10;
        }
        return encodeLowHighInts(value, length);
    }

    public static long parseIntSafely(CharSequence sequence, int lo, int hi) throws NumericException {
        if (sequence == null || lo == hi) {
            throw NumericException.instance().put("empty number string");
        }

        boolean negative = sequence.charAt(lo) == '-';
        int index = negative ? lo + 1 : lo;
        if (index >= hi || notDigit(sequence.charAt(index))) {
            throw NumericException.instance().put("not a number: ").put(sequence);
        }

        int value = 0;
        for (; index < hi; index++) {
            char c = sequence.charAt(index);
            if (notDigit(c)) {
                break;
            }
            int next = (value << 3) + (value << 1) - (c - '0');
            if (next > value) {
                throw NumericException.instance().put("number overflow");
            }
            value = next;
        }

        if (value == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance().put("number overflow");
        }
        return encodeLowHighInts(negative ? value : -value, index - lo);
    }

    public static long parseLong000000Greedy(CharSequence sequence, int lo, int hi) throws NumericException {
        long parsed = parseIntSafely(sequence, lo, hi);
        int length = decodeHighInt(parsed);
        int value = decodeLowInt(parsed);
        if (length > 6) {
            throw NumericException.instance().put("number overflow");
        }
        for (int i = length; i < 6; i++) {
            value *= 10;
        }
        return encodeLowHighInts(value, length);
    }
}
