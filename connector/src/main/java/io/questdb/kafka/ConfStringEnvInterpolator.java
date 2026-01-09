package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigException;

import java.util.function.Function;

/**
 * Expands environment variable references in configuration strings.
 * <p>
 * Supports {@code ${VAR}} syntax for environment variable expansion.
 * Use {@code $$} to escape a literal dollar sign.
 * Only the {@code ${VAR}} syntax is supported; {@code $VAR} without braces is not expanded.
 * </p>
 * <p>
 * Note: Expansion is not recursive. If an environment variable's value contains
 * {@code ${OTHER}}, that nested reference will not be expanded and will appear
 * literally in the output.
 * </p>
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code ${TOKEN}} - replaced with value of TOKEN environment variable</li>
 *   <li>{@code $${VAR}} - becomes literal {@code ${VAR}} (escaped)</li>
 *   <li>{@code $$${VAR}} - becomes {@code $} followed by value of VAR
 *       (first {@code $$} escapes to {@code $}, then {@code ${VAR}} is expanded)</li>
 *   <li>{@code $$} - becomes literal {@code $}</li>
 *   <li>{@code $VAR} - stays as literal {@code $VAR} (braces required)</li>
 * </ul>
 * </p>
 */
public final class ConfStringEnvInterpolator {

    private ConfStringEnvInterpolator() {
    }

    /**
     * Expands {@code ${VAR}} patterns in the input string with environment variable values.
     *
     * @param input the configuration string potentially containing {@code ${VAR}} patterns
     * @return the string with all {@code ${VAR}} patterns replaced, or null if input is null
     * @throws ConfigException if a referenced environment variable is not defined,
     *                         if a variable reference is malformed (unclosed brace, empty name),
     *                         or if a variable name contains invalid characters
     */
    public static String expand(String input) {
        return expand(input, System::getenv);
    }

    /**
     * Expands {@code ${VAR}} patterns in the input string using the provided environment lookup.
     * Package-private for testing with mock environment.
     *
     * @param input     the configuration string potentially containing {@code ${VAR}} patterns
     * @param envLookup function to look up environment variable values
     * @return the string with all {@code ${VAR}} patterns replaced, or null if input is null
     * @throws ConfigException if a referenced environment variable is not defined,
     *                         if a variable reference is malformed (unclosed brace, empty name),
     *                         or if a variable name contains invalid characters
     */
    static String expand(String input, Function<String, String> envLookup) {
        if (input == null) {
            return null;
        }

        int len = input.length();
        if (len == 0) {
            return input;
        }

        // Quick check: if no $ in string, return as-is
        if (input.indexOf('$') == -1) {
            return input;
        }

        StringBuilder result = new StringBuilder(len);
        int i = 0;

        while (i < len) {
            char c = input.charAt(i);

            if (c == '$') {
                if (i + 1 < len) {
                    char next = input.charAt(i + 1);
                    if (next == '$') {
                        // Escaped dollar: $$ -> $
                        result.append('$');
                        i += 2;
                    } else if (next == '{') {
                        // Variable reference: ${VAR}
                        int varStart = i + 2;
                        int varEnd = findClosingBrace(input, varStart);
                        if (varEnd == -1) {
                            throw new ConfigException("Unclosed variable reference starting at position " + i);
                        }
                        String varName = input.substring(varStart, varEnd);
                        if (varName.isEmpty()) {
                            throw new ConfigException("Empty variable name at position " + i);
                        }
                        validateVariableName(varName, i);
                        String value = envLookup.apply(varName);
                        if (value == null) {
                            throw new ConfigException("Environment variable '" + varName + "' is not defined");
                        }
                        result.append(value);
                        i = varEnd + 1;
                    } else {
                        // Just a dollar sign not followed by $ or {
                        result.append('$');
                        i++;
                    }
                } else {
                    // Dollar at end of string
                    result.append('$');
                    i++;
                }
            } else {
                result.append(c);
                i++;
            }
        }

        return result.toString();
    }

    private static int findClosingBrace(String input, int start) {
        return input.indexOf('}', start);
    }

    private static void validateVariableName(String varName, int refStartPos) {
        for (int i = 0; i < varName.length(); i++) {
            char c = varName.charAt(i);
            if (i == 0) {
                if (!isValidFirstChar(c)) {
                    throw new ConfigException("Invalid character '" + c + "' in variable name at position " + (refStartPos + 2 + i));
                }
            } else {
                if (!isValidSubsequentChar(c)) {
                    throw new ConfigException("Invalid character '" + c + "' in variable name at position " + (refStartPos + 2 + i));
                }
            }
        }
    }

    private static boolean isValidFirstChar(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isValidSubsequentChar(char c) {
        return isValidFirstChar(c) || (c >= '0' && c <= '9');
    }
}
