package io.questdb.kafka.compat;

import io.questdb.client.std.NumericException;
import io.questdb.kafka.compat.datetime.DateLocale;
import io.questdb.kafka.compat.datetime.DateLocaleFactory;
import io.questdb.kafka.compat.datetime.TimeZoneRuleFactory;
import io.questdb.kafka.compat.datetime.TimeZoneRules;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DateLocaleTest {

    @Test
    void testEnLocaleExists() {
        assertNotNull(DateLocaleFactory.EN_LOCALE);
    }

    @Test
    void testGetLocale() {
        DateLocale en = DateLocaleFactory.INSTANCE.getLocale("en");
        assertNotNull(en);
    }

    @Test
    void testMonthNames() {
        DateLocale en = DateLocaleFactory.EN_LOCALE;
        // months are 0-indexed in the locale
        assertEquals("January", en.getMonth(0));
        assertEquals("December", en.getMonth(11));
    }

    @Test
    void testShortMonthNames() {
        DateLocale en = DateLocaleFactory.EN_LOCALE;
        assertEquals("Jan", en.getShortMonth(0));
        assertEquals("Dec", en.getShortMonth(11));
    }

    @Test
    void testWeekdayNames() {
        DateLocale en = DateLocaleFactory.EN_LOCALE;
        // weekdays: index 1 = Sunday, 2 = Monday, etc. in Java DateFormatSymbols
        assertNotNull(en.getWeekday(1));
    }

    @Test
    void testMatchMonth() throws NumericException {
        DateLocale en = DateLocaleFactory.EN_LOCALE;
        // matchMonth returns encoded (index, length)
        long result = en.matchMonth("March", 0, 5);
        assertTrue(result != 0);
    }

    @Test
    void testMatchZone() throws NumericException {
        DateLocale en = DateLocaleFactory.EN_LOCALE;
        long result = en.matchZone("UTC", 0, 3);
        assertTrue(result != 0);
    }

    @Test
    void testTimeZoneRuleFactoryInstance() {
        TimeZoneRuleFactory factory = TimeZoneRuleFactory.INSTANCE;
        assertNotNull(factory);
        // UTC must be resolvable
        int idx = factory.getTimeZoneRulesIndex("UTC");
        assertTrue(idx >= 0);
    }

    @Test
    void testTimeZoneRulesOffset() {
        TimeZoneRuleFactory factory = TimeZoneRuleFactory.INSTANCE;
        int idx = factory.getTimeZoneRulesIndex("Europe/London");
        assertTrue(idx >= 0);
        TimeZoneRules rules = factory.getTimeZoneRulesQuick(idx, TimeZoneRuleFactory.RESOLUTION_MICROS);
        assertNotNull(rules);
    }
}
