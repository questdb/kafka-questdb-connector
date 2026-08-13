package io.questdb.kafka.compat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CharsTest {

    @Test
    void testEqualsAgainstSlice() {
        assertTrue(Chars.equals("bar", "foo-bar-baz", 4, 7));
        assertTrue(Chars.equals("", "abc", 1, 1));
        assertFalse(Chars.equals("bar", "foo-baz", 4, 7));
        assertFalse(Chars.equals("bar", "foo-bar-baz", 4, 8));
    }

    @Test
    void testEqualsSlices() {
        assertTrue(Chars.equals("xxbarzz", 2, 5, "--bar--", 2, 5));
        assertFalse(Chars.equals("xxbarzz", 2, 5, "--baz--", 2, 5));
        assertFalse(Chars.equals("xxbarzz", 2, 5, "--bars--", 2, 6));
    }

    @Test
    void testNoMatchComparesLowerCaseLeftToRight() {
        assertFalse(Chars.noMatch("FoO bAr", 0, 7, "--foo bar--", 2, 9));
        assertTrue(Chars.noMatch("foo baz", 0, 7, "--foo bar--", 2, 9));
        assertTrue(Chars.noMatch("foo", 0, 3, "foobar", 0, 6));
        assertTrue(Chars.noMatch("foobar", 0, 6, "foo", 0, 3));
        assertFalse(Chars.noMatch("", 0, 0, "", 0, 0));
    }
}
