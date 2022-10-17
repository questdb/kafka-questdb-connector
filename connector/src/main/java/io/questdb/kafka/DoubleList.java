package io.questdb.kafka;

final class DoubleList {
    private double[] buffer;
    private int pos = 0;

    DoubleList(int initialCapacity) {
        buffer = new double[initialCapacity];
    }

    void add(double value) {
        ensureCapacity0(pos + 1);
        buffer[pos++] = value;
    }

    double get(int index) {
        if (index >= pos) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return buffer[index];
    }

    void clear() {
        pos = 0;
    }

    void ensureCapacity0(int i) {
        if (i > buffer.length) {
            double[] newBuffer = new double[buffer.length << 1];
            System.arraycopy(buffer, 0, newBuffer, 0, pos);
            buffer = newBuffer;
        }
    }
}
