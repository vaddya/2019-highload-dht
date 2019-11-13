package ru.mail.polis.dao.vaddya.flush;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

public interface FlushListener {
    void flushed(
            int generation,
            @NotNull SSTable ssTable);
}
