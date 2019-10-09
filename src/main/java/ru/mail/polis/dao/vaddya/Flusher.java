package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;

interface Flusher {
    void flush(int generation, @NotNull Table table);
}
