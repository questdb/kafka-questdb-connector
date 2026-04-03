package io.questdb.kafka.compat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ObjIntHashMapTest {

    @Test
    void testPutAndGet() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        map.put("foo", 1);
        map.put("bar", 2);
        assertEquals(1, map.get("foo"));
        assertEquals(2, map.get("bar"));
    }

    @Test
    void testKeyIndex() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        int idx = map.keyIndex("foo");
        assertTrue(idx >= 0);
        map.putAt(idx, "foo", 42);
        assertEquals(42, map.get("foo"));
    }

    @Test
    void testClear() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        map.put("a", 1);
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    void testGrowth() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        for (int i = 0; i < 500; i++) {
            map.put("key" + i, i);
        }
        assertEquals(500, map.size());
        for (int i = 0; i < 500; i++) {
            assertEquals(i, map.get("key" + i));
        }
    }

    @Test
    void testIterator() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        int count = 0;
        for (ObjIntHashMap.Entry<String> entry : map) {
            assertNotNull(entry.key);
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    void testPutIfAbsent() {
        ObjIntHashMap<String> map = new ObjIntHashMap<>();
        assertTrue(map.putIfAbsent("foo", 1));
        assertFalse(map.putIfAbsent("foo", 2));
        assertEquals(1, map.get("foo"));
    }
}
