package io.questdb.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class TwoPartCharSequenceTest {

    @Test
    public void testLength() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("abc");
        cs.add("de");
        assertEquals(5, cs.length());
    }

    @Test
    public void testCharAt_firstPart() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("hello");
        cs.add("world");
        assertEquals('h', cs.charAt(0));
        assertEquals('e', cs.charAt(1));
        assertEquals('o', cs.charAt(4));
    }

    @Test
    public void testCharAt_secondPart() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("hello");
        cs.add("world");
        assertEquals('w', cs.charAt(5));
        assertEquals('o', cs.charAt(6));
        assertEquals('d', cs.charAt(9));
    }

    @Test
    public void testCharAt_atBoundary() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("abc");
        cs.add("def");
        assertEquals('c', cs.charAt(2));
        assertEquals('d', cs.charAt(3));
    }

    @Test
    public void testCharAt_outOfBounds() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("ab");
        cs.add("cd");
        assertThrows(IndexOutOfBoundsException.class, () -> cs.charAt(4));
        assertThrows(IndexOutOfBoundsException.class, () -> cs.charAt(10));
    }

    @Test
    public void testToString() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("20260202");
        cs.add("135010207");
        assertEquals("20260202135010207", cs.toString());
    }

    @Test
    public void testSubSequence() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("abc");
        cs.add("def");
        assertEquals("bcde", cs.subSequence(1, 5).toString());
    }

    @Test
    public void testReset() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("abc");
        cs.add("def");
        assertEquals(6, cs.length());

        cs.reset();
        cs.add("xy");
        cs.add("z");
        assertEquals(3, cs.length());
        assertEquals("xyz", cs.toString());
    }

    @Test
    public void testEmptyParts() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("");
        cs.add("abc");
        assertEquals(3, cs.length());
        assertEquals("abc", cs.toString());
        assertEquals('a', cs.charAt(0));
    }

    @Test
    public void testBothPartsEmpty() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("");
        cs.add("");
        assertEquals(0, cs.length());
        assertEquals("", cs.toString());
    }

    @Test
    public void testTypicalDateTimeUseCase() {
        TwoPartCharSequence cs = new TwoPartCharSequence();
        cs.add("20260202");
        cs.add("135010207");
        // "20260202" indices: 2(0) 0(1) 2(2) 6(3) 0(4) 2(5) 0(6) 2(7)
        // "135010207" indices: 1(8) 3(9) 5(10) 0(11) 1(12) 0(13) 2(14) 0(15) 7(16)

        assertEquals(17, cs.length());
        assertEquals('2', cs.charAt(0));
        assertEquals('2', cs.charAt(7));  // last char of date
        assertEquals('1', cs.charAt(8));  // first char of time
        assertEquals('7', cs.charAt(16)); // last char
        assertEquals("20260202135010207", cs.toString());
    }
}