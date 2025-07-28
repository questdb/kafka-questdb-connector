package io.questdb.kafka;

import io.questdb.std.str.StringSink;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class Templating {
    private Templating() {
    }

    static Function<SinkRecord, ? extends CharSequence> newTableTableFn(String template) {
        if (template == null || template.isEmpty()) {
            return SinkRecord::topic;
        }
        int currentPos = 0;
        List<Function<SinkRecord, String>> partials = null;
        for (;;) {
            int templateStartPos = template.indexOf("${", currentPos);
            if (templateStartPos == -1) {
                break;
            }
            int templateEndPos = template.indexOf('}', templateStartPos + 2);
            if (templateEndPos == -1) {
                throw new ConnectException("Unbalanced brackets in a table template, missing closing '}', table template: '" + template + "'");
            }
            int nextTemplateStartPos = template.indexOf("${", templateStartPos + 1);
            if (nextTemplateStartPos != -1 && nextTemplateStartPos < templateEndPos) {
                throw new ConnectException("Nesting templates in a table name are not supported, table template: '" + template + "'");
            }
            String templateName = template.substring(templateStartPos + 2, templateEndPos);
            if (templateName.isEmpty()) {
                throw new ConnectException("Empty template in table name, table template: '" + template + "'");
            }
            if (partials == null) {
                partials = new ArrayList<>();
            }
            String literal = template.substring(currentPos, templateStartPos);
            if (!literal.isEmpty()) {
                partials.add(record -> literal);
            }
            switch (templateName) {
                case "topic": {
                    partials.add(SinkRecord::topic);
                    break;
                }
                case "key": {
                    partials.add(record -> record.key() == null ? "null" : record.key().toString());
                    break;
                }
                case "partition": {
                    // assumption: sink records always have a non-null kafkaPartition()
                    partials.add(record -> String.valueOf(record.kafkaPartition()));
                    break;
                }
                default: {
                    throw new ConnectException("Unknown template in table name, table template: '" + template + "'");
                }
            }
            currentPos = templateEndPos + 1;
        }
        if (partials == null) {
            return record -> template;
        }
        String literal = template.substring(currentPos);
        if (!literal.isEmpty()) {
            partials.add(record -> literal);
        }
        List<Function<SinkRecord, String>> finalPartials = partials;
        StringSink sink = new StringSink();
        return record -> {
            sink.clear();
            for (Function<SinkRecord, String> partial : finalPartials) {
                sink.put(partial.apply(record));
            }
            return sink;
        };
    }
}
