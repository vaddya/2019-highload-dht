package ru.mail.polis.dao.vaddya.memtable;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.TableEntry;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
final class MemTableImpl implements MemTable {
    private final NavigableMap<ByteBuffer, TableEntry> table = new ConcurrentSkipListMap<>();
    private final AtomicInteger currentSize = new AtomicInteger();

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return table.tailMap(from).values().iterator();
    }

    @Override
    public long sizeInBytes() {
        return currentSize.get();
    }

    @Override
    public int count() {
        return table.size();
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        table.put(key, TableEntry.upsert(key, value));
        currentSize.addAndGet(Integer.BYTES + key.remaining() + Long.BYTES + Integer.BYTES + value.remaining());
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        table.put(key, TableEntry.delete(key));
        currentSize.addAndGet(Integer.BYTES + key.remaining() + Long.BYTES);
    }

    @Override
    public void clear() {
        table.clear();
        currentSize.set(0);
    }
}
