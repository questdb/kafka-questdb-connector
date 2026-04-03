package io.questdb.kafka.compat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CharSequenceIntHashMapTest {

    @Test
    void testPutAndGet() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        map.put("foo", 1);
        map.put("bar", 2);
        assertEquals(1, map.get("foo"));
        assertEquals(2, map.get("bar"));
        assertEquals(CharSequenceIntHashMap.NO_ENTRY_VALUE, map.get("baz"));
    }

    @Test
    void testKeyIndex() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        int idx = map.keyIndex("foo");
        assertTrue(idx >= 0, "key should not exist yet");
        map.putAt(idx, "foo", 42);
        assertEquals(42, map.get("foo"));
        int idx2 = map.keyIndex("foo");
        assertTrue(idx2 < 0, "key should exist now");
    }

    @Test
    void testClear() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        map.put("a", 1);
        map.put("b", 2);
        map.clear();
        assertEquals(CharSequenceIntHashMap.NO_ENTRY_VALUE, map.get("a"));
    }

    @Test
    void testGrowth() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap(4);
        for (int i = 0; i < 1000; i++) {
            map.put("key" + i, i);
        }
        for (int i = 0; i < 1000; i++) {
            assertEquals(i, map.get("key" + i));
        }
    }

    @Test
    void testOverwrite() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        map.put("foo", 1);
        map.put("foo", 2);
        assertEquals(2, map.get("foo"));
    }

    @Test
    void testPutIfAbsent() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        map.putIfAbsent("foo", 1);
        map.putIfAbsent("foo", 2);
        assertEquals(1, map.get("foo"));
    }

    @Test
    void testInc() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        map.put("counter", 10);
        map.inc("counter");
        assertEquals(11, map.get("counter"));
    }

    @Test
    void testKeys() {
        CharSequenceIntHashMap map = new CharSequenceIntHashMap();
        map.put("a", 1);
        map.put("b", 2);
        assertEquals(2, map.keys().size());
    }
}
