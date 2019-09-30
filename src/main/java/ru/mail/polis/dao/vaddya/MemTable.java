package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

final class MemTable implements Table {
    private final NavigableMap<ByteBuffer, TableEntry> table = new TreeMap<>();
    private int currentSize;

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return table.tailMap(from).values().iterator();
    }

    @Override
    public int currentSize() {
        return currentSize;
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        table.put(key, TableEntry.upsert(key, value));
        currentSize += Integer.BYTES + key.remaining() + Long.BYTES + Integer.BYTES + value.remaining();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        table.put(key, TableEntry.delete(key));
        currentSize += Integer.BYTES + key.remaining() + Long.BYTES;
    }

    @Override
    public void clear() {
        table.clear();
        currentSize = 0;
    }
}
