package io.questdb.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class CompositeCharSequenceTest {

    @Test
    public void testLength_singleSegment() {
        CompositeCharSequence cs = new CompositeCharSequence(2);
        cs.add("hello");
        assertEquals(5, cs.length());
    }

    @Test
    public void testLength_multipleSegments() {
        CompositeCharSequence cs = new CompositeCharSequence(3);
        cs.add("abc");
        cs.add("de");
        cs.add("f");
        assertEquals(6, cs.length());
    }

    @Test
    public void testCharAt_singleSegment() {
        CompositeCharSequence cs = new CompositeCharSequence(2);
        cs.add("hello");
        assertEquals('h', cs.charAt(0));
        assertEquals('e', cs.charAt(1));
        assertEquals('o', cs.charAt(4));
    }

    @Test
    public void testCharAt_acrossSegmentBoundaries() {
        CompositeCharSequence cs = new CompositeCharSequence(3);
        cs.add("abc");
        cs.add("de");
        cs.add("f");
        // segment 0: a(0), b(1), c(2)
        // segment 1: d(3), e(4)
        // segment 2: f(5)
        assertEquals('a', cs.charAt(0));
        assertEquals('c', cs.charAt(2));
        assertEquals('d', cs.charAt(3));
        assertEquals('e', cs.charAt(4));
        assertEquals('f', cs.charAt(5));
    }

    @Test
    public void testCharAt_outOfBounds() {
        CompositeCharSequence cs = new CompositeCharSequence(2);
        cs.add("ab");
        cs.add("cd");
        assertThrows(IndexOutOfBoundsException.class, () -> cs.charAt(4));
    }

    @Test
    public void testToString_multipleSegments() {
        CompositeCharSequence cs = new CompositeCharSequence(3);
        cs.add("20260202");
        cs.add("135010");
        cs.add("207");
        assertEquals("20260202135010207", cs.toString());
    }

    @Test
    public void testSubSequence() {
        CompositeCharSequence cs = new CompositeCharSequence(2);
        cs.add("abc");
        cs.add("def");
        assertEquals("bcde", cs.subSequence(1, 5).toString());
    }

    @Test
    public void testReset() {
        CompositeCharSequence cs = new CompositeCharSequence(2);
        cs.add("abc");
        cs.add("def");
        assertEquals(6, cs.length());

        cs.reset();
        assertEquals(0, cs.length());

        cs.add("xy");
        assertEquals(2, cs.length());
        assertEquals("xy", cs.toString());
    }

    @Test
    public void testEmptySegment() {
        CompositeCharSequence cs = new CompositeCharSequence(3);
        cs.add("");
        cs.add("abc");
        cs.add("");
        assertEquals(3, cs.length());
        assertEquals("abc", cs.toString());
        assertEquals('a', cs.charAt(0));
    }

    @Test
    public void testNoSegments() {
        CompositeCharSequence cs = new CompositeCharSequence(2);
        assertEquals(0, cs.length());
        assertEquals("", cs.toString());
    }
}