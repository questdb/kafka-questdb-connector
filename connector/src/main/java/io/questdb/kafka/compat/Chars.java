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

public final class Chars {
    private Chars() {
    }

    public static boolean equals(CharSequence left, CharSequence right, int rightLo, int rightHi) {
        return equals(left, 0, left.length(), right, rightLo, rightHi);
    }

    public static boolean equals(
            CharSequence left,
            int leftLo,
            int leftHi,
            CharSequence right,
            int rightLo,
            int rightHi
    ) {
        int length = leftHi - leftLo;
        if (length != rightHi - rightLo) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (left.charAt(leftLo + i) != right.charAt(rightLo + i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean noMatch(
            CharSequence left,
            int leftLo,
            int leftHi,
            CharSequence right,
            int rightLo,
            int rightHi
    ) {
        while (leftLo < leftHi && rightLo < rightHi) {
            if (Character.toLowerCase(left.charAt(leftLo++)) != right.charAt(rightLo++)) {
                return true;
            }
        }
        return leftLo != leftHi || rightLo != rightHi;
    }
}
