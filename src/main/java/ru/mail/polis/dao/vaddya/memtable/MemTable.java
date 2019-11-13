package ru.mail.polis.dao.vaddya.memtable;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import java.nio.ByteBuffer;

public interface MemTable extends SSTable {
    /**
     * Insert a value into the table using the given key.
     */
    void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value);

    /**
     * Remove a value from the table using the given key.
     */
    void remove(@NotNull final ByteBuffer key);

    /**
     * Perform table clearing.
     */
    void clear();
}
