package io.questdb.kafka.compat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BoolListTest {

    @Test
    void testAddAndGet() {
        BoolList list = new BoolList();
        list.add(true);
        list.add(false);
        list.add(true);
        assertEquals(3, list.size());
        assertTrue(list.get(0));
        assertFalse(list.get(1));
        assertTrue(list.get(2));
    }

    @Test
    void testClear() {
        BoolList list = new BoolList();
        list.add(true);
        list.add(false);
        list.clear();
        assertEquals(0, list.size());
    }

    @Test
    void testGrowBeyondInitialCapacity() {
        BoolList list = new BoolList(4);
        for (int i = 0; i < 100; i++) {
            list.add(i % 3 == 0);
        }
        assertEquals(100, list.size());
        for (int i = 0; i < 100; i++) {
            assertEquals(i % 3 == 0, list.get(i));
        }
    }

    @Test
    void testGetTrueCount() {
        BoolList list = new BoolList();
        list.add(true);
        list.add(false);
        list.add(true);
        list.add(true);
        assertEquals(3, list.getTrueCount());
    }

    @Test
    void testSetAndReplace() {
        BoolList list = new BoolList();
        list.add(true);
        list.add(true);
        list.set(1, false);
        assertFalse(list.get(1));
        boolean old = list.replace(0, false);
        assertTrue(old);
        assertFalse(list.get(0));
    }

    @Test
    void testExtendAndSet() {
        BoolList list = new BoolList();
        list.extendAndSet(5, true);
        assertEquals(6, list.size());
        assertFalse(list.get(0));
        assertTrue(list.get(5));
    }

    @Test
    void testEquals() {
        BoolList a = new BoolList();
        BoolList b = new BoolList();
        a.add(true);
        a.add(false);
        b.add(true);
        b.add(false);
        assertEquals(a, b);
        b.add(true);
        assertNotEquals(a, b);
    }
}
