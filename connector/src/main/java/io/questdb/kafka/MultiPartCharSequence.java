package io.questdb.kafka;

interface MultiPartCharSequence extends CharSequence {
    void reset();
    void add(CharSequence part);
}
