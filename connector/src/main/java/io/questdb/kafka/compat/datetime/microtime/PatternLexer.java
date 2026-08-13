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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

final class PatternLexer {
    private static final Comparator<String> LONGEST_FIRST = (a, b) -> b.length() - a.length();
    private final List<String> symbols = new ArrayList<>();
    private CharSequence input;
    private int hi;
    private int pos;

    void defineSymbol(String symbol) {
        symbols.add(symbol);
        symbols.sort(LONGEST_FIRST);
    }

    boolean hasNext() {
        return pos < hi;
    }

    CharSequence next() {
        int start = pos;
        while (pos < hi) {
            char c = input.charAt(pos);
            if (c == '\'' || c == '"' || c == '`') {
                int quoteStart = pos++;
                while (pos < hi) {
                    if (input.charAt(pos++) == c) {
                        if (c != '`' && pos < hi && input.charAt(pos) == c) {
                            pos++;
                        } else {
                            return input.subSequence(start, pos);
                        }
                    }
                }

                // Match GenericLexer semantics for an unmatched quote: emit
                // the text before it, then emit the quote on the next call.
                // When the quote starts the token, emit it immediately and
                // resume tokenizing the remaining pattern.
                if (quoteStart == start) {
                    pos = quoteStart + 1;
                    return input.subSequence(quoteStart, pos);
                }
                pos = quoteStart;
                return input.subSequence(start, quoteStart);
            }

            String symbol = findSymbol();
            if (symbol != null) {
                if (pos == start) {
                    pos += symbol.length();
                    return symbol;
                }
                return input.subSequence(start, pos);
            }
            pos++;
        }
        return input.subSequence(start, pos);
    }

    void of(CharSequence input, int lo, int hi) {
        this.input = input;
        this.pos = lo;
        this.hi = hi;
    }

    private String findSymbol() {
        for (String symbol : symbols) {
            int end = pos + symbol.length();
            if (end <= hi && matches(symbol, pos)) {
                return symbol;
            }
        }
        return null;
    }

    private boolean matches(String symbol, int offset) {
        for (int i = 0, n = symbol.length(); i < n; i++) {
            if (input.charAt(offset + i) != symbol.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
