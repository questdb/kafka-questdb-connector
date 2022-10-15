package io.questdb.kafka;

public class DoubleList {
    private double[] buffer;
    private int pos = 0;

    public DoubleList() {
        buffer = new double[4];
    }

    public void add(double value) {
        ensureCapacity0(pos + 1);
        buffer[pos++] = value;
    }

    public double get(int index) {
        if (index >= pos) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return buffer[index];
    }

    public void clear() {
        pos = 0;
    }

    private void ensureCapacity0(int i) {
        if (i > buffer.length) {
            double[] newBuffer = new double[buffer.length << 1];
            System.arraycopy(buffer, 0, newBuffer, 0, pos);
            buffer = newBuffer;
        }
    }
}
