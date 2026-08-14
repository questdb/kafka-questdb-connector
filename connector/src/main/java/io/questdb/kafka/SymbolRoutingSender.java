package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.std.bytes.DirectByteSlice;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

/**
 * Routes the columns named by {@code symbols} to {@link Sender#symbol} and passes
 * everything else straight through.
 * <p>
 * {@link BufferingSender} exists only because ILP requires every symbol to be
 * written before any other column ("symbols must be written before any other
 * column types"), which forces it to buffer a whole row and replay it in order.
 * QWP has no such ordering rule - symbols and fields may interleave freely - so
 * on that transport the buffering is pure overhead.
 */
final class SymbolRoutingSender implements Sender {
    private final Sender sender;
    private final Set<String> symbolColumns = new HashSet<>();

    SymbolRoutingSender(Sender sender, String symbolColumns) {
        this.sender = sender;
        if (symbolColumns != null) {
            for (String symbolColumn : symbolColumns.split(",")) {
                this.symbolColumns.add(symbolColumn.trim());
            }
        }
    }

    private boolean isSymbol(CharSequence name) {
        return symbolColumns.contains(name.toString());
    }

    @Override
    public Sender table(CharSequence table) {
        sender.table(table);
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        sender.symbol(name, value);
        return this;
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        if (isSymbol(name)) {
            sender.symbol(name, value);
        } else {
            sender.stringColumn(name, value);
        }
        return this;
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        if (isSymbol(name)) {
            sender.symbol(name, String.valueOf(value));
        } else {
            sender.longColumn(name, value);
        }
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        if (isSymbol(name)) {
            sender.symbol(name, String.valueOf(value));
        } else {
            sender.doubleColumn(name, value);
        }
        return this;
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        if (isSymbol(name)) {
            sender.symbol(name, String.valueOf(value));
        } else {
            sender.boolColumn(name, value);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        if (isSymbol(name)) {
            sender.symbol(name, String.valueOf(value));
        } else {
            sender.timestampColumn(name, value, unit);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant instant) {
        sender.timestampColumn(name, instant);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, double[] values) {
        sender.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, double[][] values) {
        sender.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, double[][][] values) {
        sender.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray values) {
        sender.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, long[] values) {
        sender.longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, long[][] values) {
        sender.longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, long[][][] values) {
        sender.longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, LongArray values) {
        sender.longArray(name, values);
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        return sender.bufferView();
    }

    @Override
    public void cancelRow() {
        sender.cancelRow();
    }

    @Override
    public void reset() {
        sender.reset();
    }

    @Override
    public void atNow() {
        sender.atNow();
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        sender.at(timestamp, unit);
    }

    @Override
    public void at(Instant instant) {
        sender.at(instant);
    }

    @Override
    public void flush() {
        sender.flush();
    }

    @Override
    public long flushAndGetSequence() {
        return sender.flushAndGetSequence();
    }

    @Override
    public long getAckedFsn() {
        return sender.getAckedFsn();
    }

    @Override
    public boolean awaitAckedFsn(long targetFsn, long timeoutMillis) {
        return sender.awaitAckedFsn(targetFsn, timeoutMillis);
    }

    @Override
    public boolean drain(long timeoutMillis) {
        return sender.drain(timeoutMillis);
    }

    @Override
    public void close() {
        sender.close();
    }
}
