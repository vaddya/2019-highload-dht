package ru.mail.polis.dao.vaddya.sstable;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public interface SSTablePool extends SSTable {
    void addTable(
            int generation,
            @NotNull SSTable table);

    void removeTable(int generation);

    void compact() throws IOException;
}
