package ru.mail.polis.dao.vaddya.flush;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.memtable.MemTable;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface Flusher extends Closeable {
    void scheduleFlush(
            int generation,
            @NotNull MemTable table);

    @NotNull
    SSTable flushEntries(
            final int generation,
            @NotNull final Iterator<TableEntry> iterator) throws IOException;
    
    void addListener(@NotNull final FlushListener flushListener);
}
