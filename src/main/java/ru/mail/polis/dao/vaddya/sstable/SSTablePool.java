package ru.mail.polis.dao.vaddya.sstable;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;

public interface SSTablePool extends SSTable, Closeable {
    /**
     * Get all tables of the table pool.
     */
    @NotNull
    Map<Integer, SSTable> tables();

    /**
     * Add a table to the table pool.
     */
    void addTable(
            int generation,
            @NotNull SSTable table) throws IOException;

    /**
     * Remove a table from the table pool.
     */
    void removeTable(int generation) throws IOException;

    /**
     * Compact the table pool.
     */
    void compact() throws IOException;

    @Override
    @NotNull
    default ByteBuffer lowest() {
        return tables()
                .values()
                .stream()
                .map(SSTable::lowest)
                .min(Comparator.naturalOrder())
                .orElseThrow();
    }

    @Override
    @NotNull
    default ByteBuffer highest() {
        return tables().values()
                .stream()
                .map(SSTable::highest)
                .max(Comparator.naturalOrder())
                .orElseThrow();
    }
    
    @Override
    default void close() {
        // do nothing
    }
}
