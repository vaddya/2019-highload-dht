package ru.mail.polis.dao.vaddya.memtable;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.flush.Flusher;
import ru.mail.polis.dao.vaddya.naming.GenerationProvider;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.stream.Collectors.toList;
import static ru.mail.polis.dao.vaddya.IteratorUtils.collapseIterators;

@ThreadSafe
public final class MemTablePoolImpl implements MemTablePool {
    private static final Logger log = LoggerFactory.getLogger(MemTablePoolImpl.class);

    private MemTable currentTable = new MemTableImpl();
    private final Map<Integer, MemTable> pendingFlush = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final GenerationProvider generationProvider;
    private final Flusher flusher;
    private final long flushThresholdInBytes;

    public MemTablePoolImpl(
            @NotNull final GenerationProvider generationProvider,
            @NotNull final Flusher flusher,
            final long flushThresholdInBytes) {
        this.flusher = flusher;
        this.generationProvider = generationProvider;
        this.flushThresholdInBytes = flushThresholdInBytes;
    }

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        final Collection<Iterator<TableEntry>> iterators;
        lock.readLock().lock();
        try {
            iterators = pendingFlush.values()
                    .stream()
                    .map(table -> table.iterator(from))
                    .collect(toList());
            iterators.add(currentTable.iterator(from));
        } finally {
            lock.readLock().unlock();
        }
        return collapseIterators(iterators);
    }

    @Override
    public int currentSize() {
        lock.readLock().lock();
        try {
            var size = currentTable.currentSize();
            for (final var table : pendingFlush.values()) {
                size += table.currentSize();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        if (stopped.get()) {
            throw new IllegalStateException("MemTable was already closed");
        }
        lock.readLock().lock();
        try {
            currentTable.upsert(key, value);
        } finally {
            lock.readLock().unlock();
        }
        if (currentTable.currentSize() > flushThresholdInBytes) { // to avoid extra call & lock
            enqueueToFlush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (stopped.get()) {
            throw new IllegalStateException("MemTable was already closed");
        }
        lock.readLock().lock();
        try {
            currentTable.remove(key);
        } finally {
            lock.readLock().unlock();
        }
        if (currentTable.currentSize() > flushThresholdInBytes) { // to avoid extra call & lock
            enqueueToFlush();
        }
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            currentTable.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueToFlush() {
        lock.writeLock().lock();
        try {
            if (currentTable.currentSize() > flushThresholdInBytes) {
                final var generation = generationProvider.nextGeneration();
                pendingFlush.put(generation, currentTable);
                flusher.scheduleFlush(generation, currentTable);
                log.debug("Table {} with size {} bytes was submitted to flush", generation, currentTable.currentSize());
                currentTable = new MemTableImpl();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        log.debug("Closing MemTablePool");
        lock.writeLock().lock();
        try {
            if (currentTable.currentSize() > 0) {
                final var generation = generationProvider.nextGeneration();
                flusher.scheduleFlush(generation, currentTable);
                log.debug("Table {} with size {} bytes was submitted to flush", generation, currentTable.currentSize());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
