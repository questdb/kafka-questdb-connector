package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class ConfStringEnvInterpolatorTest {

    @Test
    public void testNoVariables() {
        String input = "http::addr=localhost;";
        assertEquals(input, expand(input, Collections.emptyMap()));
    }

    @Test
    public void testSingleVariable() {
        Map<String, String> env = new HashMap<>();
        env.put("TOKEN", "secret");
        assertEquals(
                "http::addr=localhost;token=secret;",
                expand("http::addr=localhost;token=${TOKEN};", env)
        );
    }

    @Test
    public void testMultipleVariables() {
        Map<String, String> env = new HashMap<>();
        env.put("PROTO", "https");
        env.put("HOST", "db");
        env.put("TOKEN", "x");
        assertEquals(
                "https::addr=db;token=x;",
                expand("${PROTO}::addr=${HOST};token=${TOKEN};", env)
        );
    }

    @Test
    public void testVariableAtStart() {
        Map<String, String> env = Collections.singletonMap("TOKEN", "abc");
        assertEquals("abc", expand("${TOKEN}", env));
    }

    @Test
    public void testVariableAtEnd() {
        Map<String, String> env = Collections.singletonMap("TOKEN", "abc");
        assertEquals("prefixabc", expand("prefix${TOKEN}", env));
    }

    @Test
    public void testAdjacentVariables() {
        Map<String, String> env = new HashMap<>();
        env.put("A", "foo");
        env.put("B", "bar");
        assertEquals("foobar", expand("${A}${B}", env));
    }

    @Test
    public void testSameVariableTwice() {
        Map<String, String> env = Collections.singletonMap("X", "val");
        assertEquals("val/val", expand("${X}/${X}", env));
    }

    @Test
    public void testEmptyEnvValue() {
        Map<String, String> env = Collections.singletonMap("TOKEN", "");
        assertEquals("token=;", expand("token=${TOKEN};", env));
    }

    @Test
    public void testWhitespaceInValue() {
        Map<String, String> env = Collections.singletonMap("TOKEN", "  spaces  ");
        assertEquals("token=  spaces  ;", expand("token=${TOKEN};", env));
    }

    @Test
    public void testEmptyInput() {
        assertEquals("", expand("", Collections.emptyMap()));
    }

    @Test
    public void testNullInput() {
        assertNull(ConfStringEnvInterpolator.expand(null, s -> null));
    }

    @Test
    public void testEscapedDollar() {
        assertEquals("price=$100", expand("price=$$100", Collections.emptyMap()));
    }

    @Test
    public void testEscapedVariable() {
        assertEquals("literal=${VAR}", expand("literal=$${VAR}", Collections.emptyMap()));
    }

    @Test
    public void testDoubleEscape() {
        assertEquals("$$", expand("$$$$", Collections.emptyMap()));
    }

    @Test
    public void testEscapeBeforeRealVar() {
        Map<String, String> env = Collections.singletonMap("VAR", "x");
        assertEquals("$x", expand("$$${VAR}", env));
    }

    @Test
    public void testMixedEscapes() {
        Map<String, String> env = Collections.singletonMap("VAR", "mid");
        assertEquals("$mid$", expand("$$${VAR}$$", env));
    }

    @Test
    public void testUndefinedVariable() {
        try {
            expand("${UNDEFINED}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Environment variable 'UNDEFINED' is not defined", e.getMessage());
        }
    }

    @Test
    public void testUnclosedBrace() {
        try {
            expand("${VAR", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Unclosed variable reference starting at position 0", e.getMessage());
        }
    }

    @Test
    public void testUnclosedBraceInMiddle() {
        try {
            expand("prefix${VAR", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Unclosed variable reference starting at position 6", e.getMessage());
        }
    }

    @Test
    public void testEmptyVarName() {
        try {
            expand("${}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Empty variable name at position 0", e.getMessage());
        }
    }

    @Test
    public void testInvalidCharHyphen() {
        try {
            expand("${VAR-NAME}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid character '-' in variable name at position 5", e.getMessage());
        }
    }

    @Test
    public void testInvalidCharDot() {
        try {
            expand("${VAR.NAME}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid character '.' in variable name at position 5", e.getMessage());
        }
    }

    @Test
    public void testInvalidCharSpace() {
        try {
            expand("${VAR NAME}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid character ' ' in variable name at position 5", e.getMessage());
        }
    }

    @Test
    public void testInvalidStartingWithDigit() {
        try {
            expand("${123VAR}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid character '1' in variable name at position 2", e.getMessage());
        }
    }

    @Test
    public void testNestedBraces() {
        // ${A${B}} - the inner ${ is treated as invalid characters in var name
        try {
            expand("${A${B}}", Collections.emptyMap());
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid character '$' in variable name at position 3", e.getMessage());
        }
    }

    @Test
    public void testDollarAtEnd() {
        assertEquals("test$", expand("test$", Collections.emptyMap()));
    }

    @Test
    public void testDollarSpace() {
        // "$ {VAR}" - dollar followed by space, not a variable reference
        Map<String, String> env = Collections.singletonMap("VAR", "x");
        assertEquals("$ {VAR}", expand("$ {VAR}", env));
    }

    @Test
    public void testJustDollar() {
        assertEquals("$", expand("$", Collections.emptyMap()));
    }

    @Test
    public void testBraceWithoutDollar() {
        assertEquals("{VAR}", expand("{VAR}", Collections.emptyMap()));
    }

    @Test
    public void testDollarWithoutBraces() {
        // $FOO is NOT expanded, only ${FOO} syntax works
        Map<String, String> env = Collections.singletonMap("FOO", "bar");
        assertEquals("$FOO", expand("$FOO", env));
    }

    @Test
    public void testNoRecursiveExpansion() {
        // Expansion is not recursive - ${OTHER} in value stays literal
        Map<String, String> env = new HashMap<>();
        env.put("TOKEN", "before${OTHER}after");
        env.put("OTHER", "should-not-appear");
        assertEquals("before${OTHER}after", expand("${TOKEN}", env));
    }

    @Test
    public void testVeryLongVarName() {
        String longName = "VERY_LONG_VARIABLE_NAME_THAT_IS_QUITE_EXTENDED";
        Map<String, String> env = Collections.singletonMap(longName, "value");
        assertEquals("value", expand("${" + longName + "}", env));
    }

    @Test
    public void testUnicodeInValue() {
        Map<String, String> env = Collections.singletonMap("U", "\u65E5\u672C\u8A9E");
        assertEquals("\u65E5\u672C\u8A9E", expand("${U}", env));
    }

    @Test
    public void testNewlineInValue() {
        Map<String, String> env = Collections.singletonMap("N", "a\nb");
        assertEquals("a\nb", expand("${N}", env));
    }

    @Test
    public void testSemicolonInValue() {
        // Note: semicolons in values may break conf string parsing - this is documented
        Map<String, String> env = Collections.singletonMap("S", "a;b");
        assertEquals("a;b", expand("${S}", env));
    }

    @Test
    public void testValidVarNameWithUnderscore() {
        Map<String, String> env = Collections.singletonMap("_VAR", "val");
        assertEquals("val", expand("${_VAR}", env));
    }

    @Test
    public void testValidVarNameWithUnderscoreInMiddle() {
        Map<String, String> env = Collections.singletonMap("VAR_NAME", "val");
        assertEquals("val", expand("${VAR_NAME}", env));
    }

    @Test
    public void testValidVarNameWithDigits() {
        Map<String, String> env = Collections.singletonMap("VAR123", "val");
        assertEquals("val", expand("${VAR123}", env));
    }

    @Test
    public void testValidVarNameLowercase() {
        Map<String, String> env = Collections.singletonMap("var", "val");
        assertEquals("val", expand("${var}", env));
    }

    @Test
    public void testValidVarNameMixedCase() {
        Map<String, String> env = Collections.singletonMap("VarName", "val");
        assertEquals("val", expand("${VarName}", env));
    }

    @Test
    public void testRealisticHttpConfig() {
        Map<String, String> env = new HashMap<>();
        env.put("QUESTDB_HOST", "questdb.default.svc.cluster.local");
        env.put("QUESTDB_TOKEN", "super-secret-token-123");

        String input = "http::addr=${QUESTDB_HOST}:9000;token=${QUESTDB_TOKEN};auto_flush_rows=1000;";
        String expected = "http::addr=questdb.default.svc.cluster.local:9000;token=super-secret-token-123;auto_flush_rows=1000;";

        assertEquals(expected, expand(input, env));
    }

    @Test
    public void testMixedLiteralAndVars() {
        Map<String, String> env = new HashMap<>();
        env.put("ENV", "prod");
        env.put("TOKEN", "secret");

        String input = "http::addr=questdb-${ENV}.internal:9000;token=${TOKEN};";
        String expected = "http::addr=questdb-prod.internal:9000;token=secret;";

        assertEquals(expected, expand(input, env));
    }

    // Helper method to simplify test code
    private static String expand(String input, Map<String, String> env) {
        return ConfStringEnvInterpolator.expand(input, env::get);
    }
}
