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

import io.questdb.kafka.compat.CharSequenceIntHashMap;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.ObjList;
import io.questdb.kafka.compat.datetime.microtime.Micros;
import io.questdb.kafka.compat.datetime.microtime.TimeZoneRulesMicros;
import io.questdb.kafka.compat.datetime.millitime.Dates;

import java.time.ZoneId;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesProvider;
import java.util.Map;

public class TimeZoneRuleFactory {
    public static final TimeZoneRuleFactory INSTANCE = new TimeZoneRuleFactory();
    public static final int RESOLUTION_MICROS = 0;
    private final ObjList<TimeZoneRules> ruleList = new ObjList<>();
    private final CharSequenceIntHashMap ruleMap = new CharSequenceIntHashMap();

    public TimeZoneRuleFactory() {
        int index = 0;
        for (String z : ZoneRulesProvider.getAvailableZoneIds()) {
            final ZoneRules rules = ZoneRulesProvider.getRules(z, true);
            ruleList.add(new TimeZoneRulesMicros(rules));
            ruleMap.put(z, index++);
        }

        for (Map.Entry<String, String> e : ZoneId.SHORT_IDS.entrySet()) {
            String key = e.getKey();
            String alias = e.getValue();

            int i = ruleMap.get(key);
            if (i == -1) {
                i = ruleMap.get(alias);
                if (i == -1) {
                    long offset = Dates.parseOffset(alias, 0, alias.length());
                    if (offset != Long.MIN_VALUE) {
                        ruleList.add(new FixedTimeZoneRule(Numbers.decodeLowInt(offset) * Micros.MINUTE_MICROS));
                        ruleMap.put(key, index++);
                    }
                } else {
                    ruleMap.put(key, i);
                }
            }
        }
    }

    public int getTimeZoneRulesIndex(CharSequence id) {
        return ruleMap.get(id);
    }

    public TimeZoneRules getTimeZoneRulesQuick(int index, int resolution) {
        return ruleList.getQuick(index + resolution);
    }
}
