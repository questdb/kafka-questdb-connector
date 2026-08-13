package io.questdb.kafka.compat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntListTest {

    @Test
    void testSetQuickReplacesExistingValue() {
        IntList list = new IntList();
        list.add(10);
        list.add(20);

        list.setQuick(1, 42);

        assertEquals(2, list.size());
        assertEquals(10, list.getQuick(0));
        assertEquals(42, list.getQuick(1));
    }

    @Test
    void testSetQuickRejectsInvalidIndexes() {
        IntList list = new IntList();
        list.add(10);

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.setQuick(-1, 1));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.setQuick(1, 1));
    }
}
