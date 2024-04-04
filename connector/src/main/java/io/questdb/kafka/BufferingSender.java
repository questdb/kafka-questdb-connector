package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.std.BoolList;
import io.questdb.std.LongList;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Allow to add regular fields before adding symbols.
 * <p>
 * Internally it buffers symbols and fields and sends them on calling <code>atNow()</code> or <code>at()</code>.
 *
 */
final class BufferingSender implements Sender {
    private static final int DEFAULT_CAPACITY = 4;

    private final Sender sender;
    private final List<CharSequence> timestampNames = new ArrayList<>(DEFAULT_CAPACITY);
    private final LongList timestampValues = new LongList(DEFAULT_CAPACITY);
    private final List<CharSequence> longNames = new ArrayList<>(DEFAULT_CAPACITY);
    private final LongList longValues = new LongList(DEFAULT_CAPACITY);
    private final List<CharSequence> doubleNames = new ArrayList<>(DEFAULT_CAPACITY);
    private final DoubleList doubleValues = new DoubleList(DEFAULT_CAPACITY);
    private final List<CharSequence> boolNames = new ArrayList<>(DEFAULT_CAPACITY);
    private final BoolList boolValues = new BoolList(DEFAULT_CAPACITY);
    private final List<CharSequence> stringNames = new ArrayList<>(DEFAULT_CAPACITY);
    private final List<CharSequence> stringValues = new ArrayList<>(DEFAULT_CAPACITY);
    private final List<CharSequence> symbolColumnNames = new ArrayList<>(DEFAULT_CAPACITY);
    private final List<CharSequence> symbolColumnValues = new ArrayList<>(DEFAULT_CAPACITY);
    private final Set<CharSequence> symbolColumns = new HashSet<>();

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
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        if (symbolColumns.contains(name)) {
            symbolColumnNames.add(name);
            symbolColumnValues.add(String.valueOf(value));
        } else {
            timestampNames.add(name);
            timestampValues.add(unitToMicros(value, unit));
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence charSequence, Instant instant) {
        return null;
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
            sender.timestampColumn(fieldName, fieldValue, ChronoUnit.MICROS);
        }
        timestampNames.clear();
        timestampValues.clear();
    }

    private static long unitToMicros(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / 1000L;
            case MICROS:
                return value;
            case MILLIS:
                return value * 1000L;
            case SECONDS:
                return value * 1_000_000L;
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        transferFields();
        sender.at(timestamp, unit);
    }

    @Override
    public void at(Instant instant) {
        throw new UnsupportedOperationException();
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
