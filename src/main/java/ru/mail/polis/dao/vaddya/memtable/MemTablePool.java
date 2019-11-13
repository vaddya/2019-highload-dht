package ru.mail.polis.dao.vaddya.memtable;

import java.io.Closeable;

public interface MemTablePool extends MemTable, Closeable {
    void flushed(final int generation);
}
