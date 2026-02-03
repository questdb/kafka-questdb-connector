package io.questdb.kafka;

final class TwoPartCharSequence implements MultiPartCharSequence {
    private CharSequence first;
    private CharSequence second;
    private boolean hasFirst;

    @Override
    public void reset() {
        first = null;
        second = null;
        hasFirst = false;
    }

    @Override
    public void add(CharSequence part) {
        if (!hasFirst) {
            first = part;
            hasFirst = true;
        } else {
            second = part;
        }
    }

    @Override
    public int length() {
        return first.length() + second.length();
    }

    @Override
    public char charAt(int index) {
        int firstLen = first.length();
        if (index < firstLen) {
            return first.charAt(index);
        }
        int secondIndex = index - firstLen;
        if (secondIndex < second.length()) {
            return second.charAt(secondIndex);
        }
        throw new IndexOutOfBoundsException("index: " + index + ", length: " + length());
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return toString().subSequence(start, end);
    }

    @Override
    public String toString() {
        return new StringBuilder(length()).append(first).append(second).toString();
    }
}
