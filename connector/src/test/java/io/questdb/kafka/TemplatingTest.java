package io.questdb.kafka;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class TemplatingTest {

    @Test
    public void testPlainTableName() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("table");
        SinkRecord record = newSinkRecord("topic", "key");
        assertTableName(fn, record, "table");
    }

    @Test
    public void testEmptyTableName() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("");
        SinkRecord record = newSinkRecord("topic", "key");
        assertTableName(fn, record, "topic");
    }

    @Test
    public void testNullTableName() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn(null);
        SinkRecord record = newSinkRecord("topic", "key");
        assertTableName(fn, record, "topic");
    }

    @Test
    public void testSimpleTopicTemplate() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("${topic}");
        SinkRecord record = newSinkRecord("mytopic", "key");
        assertTableName(fn, record, "mytopic");
    }

    @Test
    public void testTopicWithKeyTemplates() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("${topic}_${key}");
        SinkRecord record = newSinkRecord("mytopic", "mykey");
        assertTableName(fn, record, "mytopic_mykey");
    }

    @Test
    public void testTopicWithNullKey() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("${topic}_${key}");
        SinkRecord record = newSinkRecord("mytopic", null);
        assertTableName(fn, record, "mytopic_null");
    }

    @Test
    public void testMissingClosingBrackets() {
        assertIllegalTemplate("${topic", "Unbalanced brackets in a table template, missing closing '}', table template: '${topic'");
    }

    @Test
    public void testOverlappingTemplates() {
        assertIllegalTemplate("${topic${key}", "Nesting templates in a table name are not supported, table template: '${topic${key}'");
    }

    @Test
    public void testEmptyTemplate() {
        assertIllegalTemplate("${}", "Empty template in table name, table template: '${}'");
    }

    @Test
    public void testIllegalTemplate() {
        assertIllegalTemplate("${unknown}", "Unknown template in table name, table template: '${unknown}'");
    }

    @Test
    public void testSuffixLiteral() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("${topic}_suffix");
        SinkRecord record = newSinkRecord("mytopic", "key");
        assertTableName(fn, record, "mytopic_suffix");
    }

    private static void assertIllegalTemplate(String template, String expectedMessage) {
        try {
            Templating.newTableTableFn(template);
            Assert.fail();
        } catch (ConnectException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }

    @Test
    public void testTopicWithEmptyKey() {
        Function<SinkRecord, ? extends CharSequence> fn = Templating.newTableTableFn("${topic}_${key}");
        SinkRecord record = newSinkRecord("mytopic", "");
        assertTableName(fn, record, "mytopic_");
    }

    private static void assertTableName(Function<SinkRecord, ? extends CharSequence> fn, SinkRecord record, String expectedTable) {
        Assert.assertEquals(expectedTable, fn.apply(record).toString());
    }

    private static SinkRecord newSinkRecord(String topic, String key) {
        return new SinkRecord(topic, 0, null, key, null, null, 0);
    }

}