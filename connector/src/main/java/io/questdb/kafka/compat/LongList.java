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

import java.util.Arrays;

public final class LongList {
    private long[] values = new long[16];
    private int size;

    public void add(long value) {
        if (size == values.length) {
            values = Arrays.copyOf(values, values.length * 2);
        }
        values[size++] = value;
    }

    public int binarySearch(long value) {
        int index = Arrays.binarySearch(values, 0, size, value);
        if (index >= 0) {
            while (index > 0 && values[index - 1] == value) {
                index--;
            }
        }
        return index;
    }

    public void clear() {
        size = 0;
    }

    public long get(int index) {
        if (index >= size) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return values[index];
    }

    public long getLast() {
        return size == 0 ? -1 : values[size - 1];
    }

    public long getQuick(int index) {
        return values[index];
    }

    public int size() {
        return size;
    }
}
