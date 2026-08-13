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

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;

import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DateLocale {
    private final String[] ampmArray;
    private final Map<Integer, List<Token>> amspms = new HashMap<>();
    private final String[] eraArray;
    private final Map<Integer, List<Token>> eras = new HashMap<>();
    private final TimeZoneRuleFactory factory;
    private final String[] monthArray;
    private final Map<Integer, List<Token>> months = new HashMap<>();
    private final String name;
    private final String[] shortMonthArray;
    private final String[] shortWeekdayArray;
    private final String[] weekdayArray;
    private final Map<Integer, List<Token>> weekdays = new HashMap<>();
    private final Map<Integer, List<Token>> zones = new HashMap<>();

    public DateLocale(String name, DateFormatSymbols symbols, TimeZoneRuleFactory timeZoneRuleFactory) {
        this.name = name;
        this.factory = timeZoneRuleFactory;
        index(monthArray = symbols.getMonths(), months);
        index(shortMonthArray = symbols.getShortMonths(), months);
        index(weekdayArray = symbols.getWeekdays(), weekdays);
        index(shortWeekdayArray = symbols.getShortWeekdays(), weekdays);
        index(ampmArray = symbols.getAmPmStrings(), amspms);
        index(eraArray = symbols.getEras(), eras);
        indexZones(symbols.getZoneStrings(), timeZoneRuleFactory);
    }

    private static void sort(Map<Integer, List<Token>> map) {
        for (List<Token> list : map.values()) {
            list.sort(Comparator.comparingInt((Token token) -> token.text.length()).reversed());
        }
    }

    public String getAMPM(int index) {
        return ampmArray[index];
    }

    public String getEra(int index) {
        return eraArray[index];
    }

    public String getMonth(int index) {
        return monthArray[index];
    }

    public String getName() {
        return name;
    }

    public String getShortMonth(int index) {
        return shortMonthArray[index];
    }

    public String getShortWeekday(int index) {
        return shortWeekdayArray[index];
    }

    public String getWeekday(int index) {
        return weekdayArray[index];
    }

    public TimeZoneRules getZoneRules(int index, int resolution) {
        return factory.getTimeZoneRulesQuick(index, resolution);
    }

    public long matchAMPM(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, amspms);
    }

    public long matchEra(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, eras);
    }

    public long matchMonth(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, months);
    }

    public long matchWeekday(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, weekdays);
    }

    public long matchZone(CharSequence content, int lo, int hi) throws NumericException {
        return findToken(content, lo, hi, zones);
    }

    private static void defineToken(String token, int pos, Map<Integer, List<Token>> map) {
        if (token == null || token.isEmpty()) {
            return;
        }

        char c0 = Character.toUpperCase(token.charAt(0));
        List<Token> list = map.computeIfAbsent((int) c0, ignored -> new ArrayList<>());
        list.add(new Token(pos, token));
    }

    private static long findToken(CharSequence content, int lo, int hi, Map<Integer, List<Token>> map) throws NumericException {
        if (lo >= hi) {
            throw NumericException.instance();
        }

        char c = Character.toUpperCase(content.charAt(lo));

        List<Token> list = map.get((int) c);
        if (list == null) {
            throw NumericException.instance();
        }

        for (int i = 0, sz = list.size(); i < sz; i++) {
            Token token = list.get(i);
            String text = token.text;
            int n = text.length();
            boolean match = n <= hi - lo;
            if (match) {
                for (int k = 1; k < n; k++) {
                    if (Character.toUpperCase(content.charAt(lo + k)) != Character.toUpperCase(text.charAt(k))) {
                        match = false;
                        break;
                    }
                }
            }

            if (match) {
                return Numbers.encodeLowHighInts(token.value, n);
            }
        }

        throw NumericException.instance();
    }

    private static void index(String[] tokens, Map<Integer, List<Token>> map) {
        for (int i = 0, n = tokens.length; i < n; i++) {
            defineToken(tokens[i], i, map);
        }
        sort(map);
    }

    private void indexZones(String[][] zones, TimeZoneRuleFactory timeZoneRuleFactory) {
        HashSet<String> cache = new HashSet<>();
        // this is a workaround a problem where UTC timezone comes nearly last
        // in this array, which gives way to Antarctica/Troll take its place

        if (cache.add("UTC")) {
            int index = timeZoneRuleFactory.getTimeZoneRulesIndex("UTC");
            if (index != -1) {
                defineToken("UTC", index, this.zones);
            }
        }

        // end of workaround

        for (int i = 0, n = zones.length; i < n; i++) {
            String[] zNames = zones[i];
            String key = zNames[0];

            int index = timeZoneRuleFactory.getTimeZoneRulesIndex(key);
            if (index == -1) {
                continue;
            }

            for (int k = 0, m = zNames.length; k < m; k++) {
                String name = zNames[k];
                // we already added this name, skip
                if (cache.add(name)) {
                    defineToken(name, index, this.zones);
                }
            }
        }
        sort(this.zones);
    }

    private static final class Token {
        private final String text;
        private final int value;

        private Token(int value, String text) {
            this.value = value;
            this.text = text;
        }
    }
}
