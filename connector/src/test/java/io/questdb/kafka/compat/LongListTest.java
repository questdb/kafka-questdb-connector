package io.questdb.kafka.compat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LongListTest {

    @Test
    void testAddGrowsAndPreservesValues() {
        LongList list = new LongList();
        for (int i = 0; i < 100; i++) {
            list.add(i * 10L);
        }

        assertEquals(100, list.size());
        for (int i = 0; i < 100; i++) {
            assertEquals(i * 10L, list.get(i));
            assertEquals(i * 10L, list.getQuick(i));
        }
        assertEquals(990L, list.getLast());
    }

    @Test
    void testBinarySearchReturnsFirstDuplicateOrInsertionPoint() {
        LongList list = new LongList();
        long[] values = {0, 2, 2, 2, 5, 8};
        for (long value : values) {
            list.add(value);
        }

        assertEquals(0, list.binarySearch(0));
        assertEquals(1, list.binarySearch(2));
        assertEquals(4, list.binarySearch(5));
        assertEquals(5, list.binarySearch(8));
        assertEquals(-1, list.binarySearch(-1));
        assertEquals(-2, list.binarySearch(1));
        assertEquals(-5, list.binarySearch(4));
        assertEquals(-7, list.binarySearch(9));
    }

    @Test
    void testClearAndReuse() {
        LongList list = new LongList();
        list.add(10);
        list.add(20);
        list.clear();

        assertEquals(0, list.size());
        assertEquals(-1, list.getLast());

        list.add(30);
        assertEquals(1, list.size());
        assertEquals(30, list.getLast());
    }

    @Test
    void testCheckedGetRejectsInvalidIndexes() {
        LongList list = new LongList();
        list.add(10);

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.get(-1));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.get(1));
    }
}
