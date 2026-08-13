package io.questdb.kafka.compat.datetime.microtime;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PatternLexerTest {

    @Test
    void testBrokenQuotedTokenAfterText() {
        assertEquals(List.of("#1234", "'"), lex("#1234'"));
        assertEquals(List.of("#1234", "\""), lex("#1234\""));
        assertEquals(List.of("#1234", "`"), lex("#1234`"));
    }

    @Test
    void testBrokenQuotedTokenBeforeText() {
        assertEquals(List.of("'", "#1234"), lex("'#1234"));
        assertEquals(List.of("\"", "#1234"), lex("\"#1234"));
        assertEquals(List.of("`", "#1234"), lex("`#1234"));
    }

    @Test
    void testBrokenQuotedTokenWithinText() {
        assertEquals(List.of("#12", "'", "34"), lex("#12'34"));
        assertEquals(List.of("#12", "\"", "34"), lex("#12\"34"));
        assertEquals(List.of("#12", "`", "34"), lex("#12`34"));
    }

    @Test
    void testQuotedTokenAndEscapedQuoteStayTogether() {
        assertEquals(List.of("a", "+", "'b''c'", "*", "abc"), lex("a+'b''c'*abc", "+", "++", "*"));
        assertEquals(List.of("''"), lex("''"));
        assertEquals(List.of("\"\""), lex("\"\""));
        assertEquals(List.of("``"), lex("``"));
    }

    @Test
    void testLongestSymbolWins() {
        assertEquals(
                List.of("+", "*", "a", "+", "b", "++", "blah-"),
                lex("+*a+b++blah-", "+", "++", "*")
        );
    }

    @Test
    void testInputRange() {
        PatternLexer lexer = new PatternLexer();
        lexer.defineSymbol("+");
        lexer.of("ignore:a+b:ignore", 7, 10);
        assertEquals(List.of("a", "+", "b"), readAll(lexer));
    }

    private static List<String> lex(String input, String... symbols) {
        PatternLexer lexer = new PatternLexer();
        for (String symbol : symbols) {
            lexer.defineSymbol(symbol);
        }
        lexer.of(input, 0, input.length());
        return readAll(lexer);
    }

    private static List<String> readAll(PatternLexer lexer) {
        List<String> tokens = new ArrayList<>();
        while (lexer.hasNext()) {
            tokens.add(lexer.next().toString());
        }
        return tokens;
    }
}
