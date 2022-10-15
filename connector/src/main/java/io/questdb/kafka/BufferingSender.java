package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.std.BoolList;
import io.questdb.std.LongList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class BufferingSender implements Sender {
    private final Sender sender;

    private List<CharSequence> timestampNames = new ArrayList<>(4);
    private LongList timestampValues = new LongList(4);

    private List<CharSequence> longNames = new ArrayList<>(4);
    private LongList longValues = new LongList(4);

    private List<CharSequence> doubleNames = new ArrayList<>(4);
    private DoubleList doubleValues = new DoubleList();

    private List<CharSequence> boolNames = new ArrayList<>(4);
    private BoolList boolValues = new BoolList(4);

    private List<CharSequence> stringNames = new ArrayList<>(4);
    private List<CharSequence> stringValues = new ArrayList<>(4);

    private List<CharSequence> symbolColumnNames = new ArrayList<>(4);
    private List<CharSequence> symbolColumnValues = new ArrayList<>(4);


    private Set<CharSequence> symbolColumns = new HashSet<>();

    BufferingSender(Sender sender, String symbolColumns) {
        this.sender = sender;
        if (symbolColumns != null) {
            for (String symbolColumn : symbolColumns.split(",")) {
                this.symbolColumns.add(symbolColumn.trim());
            }
        }
    }

    @Override
    public Sender table(CharSequence table) {
        return sender.table(table);
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        if (symbolColumns.contains(name)) {
            symbolColumnNames.add(name);
            symbolColumnValues.add(String.valueOf(value));
        } else {
            longNames.add(name);
            longValues.add(value);
        }
        return this;
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        if (symbolColumns.contains(name)) {
            symbolColumnNames.add(name);
            symbolColumnValues.add(value);
        } else {
            stringNames.add(name);
            stringValues.add(value);
        }
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        if (symbolColumns.contains(name)) {
            symbolColumnNames.add(name);
            symbolColumnValues.add(String.valueOf(value));
        } else {
            doubleNames.add(name);
            doubleValues.add(value);
        }
        return this;
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        if (symbolColumns.contains(name)) {
            symbolColumnNames.add(name);
            symbolColumnValues.add(String.valueOf(value));
        } else {
            boolNames.add(name);
            boolValues.add(value);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value) {
        if (symbolColumns.contains(name)) {
            symbolColumnNames.add(name);
            symbolColumnValues.add(String.valueOf(value));
        } else {
            timestampNames.add(name);
            timestampValues.add(value);
        }
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        symbolColumnNames.add(name);
        symbolColumnValues.add(value);
        return this;
    }

    @Override
    public void atNow() {
        transferFields();
        sender.atNow();
    }

    private void transferFields() {
        for (int i = 0, n = symbolColumnNames.size(); i < n; i++) {
            CharSequence symbolName = symbolColumnNames.get(i);
            CharSequence symbolValue = symbolColumnValues.get(i);
            sender.symbol(symbolName, symbolValue);
        }
        symbolColumnNames.clear();
        symbolColumnValues.clear();

        for (int i = 0, n = stringNames.size(); i < n; i++) {
            CharSequence fieldName = stringNames.get(i);
            CharSequence fieldValue = stringValues.get(i);
            sender.stringColumn(fieldName, fieldValue);
        }
        stringNames.clear();
        stringValues.clear();

        for (int i = 0, n = longNames.size(); i < n; i++) {
            CharSequence fieldName = longNames.get(i);
            long fieldValue = longValues.get(i);
            sender.longColumn(fieldName, fieldValue);
        }
        longNames.clear();
        longValues.clear();

        for (int i = 0, n = doubleNames.size(); i < n; i++) {
            CharSequence fieldName = doubleNames.get(i);
            double fieldValue = doubleValues.get(i);
            sender.doubleColumn(fieldName, fieldValue);
        }
        doubleNames.clear();
        doubleValues.clear();

        for (int i = 0, n = boolNames.size(); i < n; i++) {
            CharSequence fieldName = boolNames.get(i);
            boolean fieldValue = boolValues.get(i);
            sender.boolColumn(fieldName, fieldValue);
        }
        boolNames.clear();
        boolValues.clear();

        for (int i = 0, n = timestampNames.size(); i < n; i++) {
            CharSequence fieldName = timestampNames.get(i);
            long fieldValue = timestampValues.get(i);
            sender.timestampColumn(fieldName, fieldValue);
        }
        timestampNames.clear();
        timestampValues.clear();
    }

    @Override
    public void at(long timestamp) {
        transferFields();
        sender.at(timestamp);
    }

    @Override
    public void flush() {
        sender.flush();
    }

    @Override
    public void close() {
        sender.close();
    }
}
