package io.questdb.kafka;

final class CompositeCharSequence implements MultiPartCharSequence {
    private CharSequence[] segments;
    private int count;

    CompositeCharSequence(int maxSegments) {
        this.segments = new CharSequence[maxSegments];
        this.count = 0;
    }

    @Override
    public void reset() {
        for (int i = 0; i < count; i++) {
            segments[i] = null;
        }
        count = 0;
    }

    @Override
    public void add(CharSequence segment) {
        segments[count++] = segment;
    }

    @Override
    public int length() {
        int len = 0;
        for (int i = 0; i < count; i++) {
            len += segments[i].length();
        }
        return len;
    }

    @Override
    public char charAt(int index) {
        int offset = 0;
        for (int i = 0; i < count; i++) {
            int segLen = segments[i].length();
            if (index < offset + segLen) {
                return segments[i].charAt(index - offset);
            }
            offset += segLen;
        }
        throw new IndexOutOfBoundsException("index: " + index + ", length: " + offset);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return toString().subSequence(start, end);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(length());
        for (int i = 0; i < count; i++) {
            sb.append(segments[i]);
        }
        return sb.toString();
    }
}
