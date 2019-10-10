package ru.mail.polis.dao.vaddya;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.dao.vaddya.IteratorUtils.collectIterators;
import static ru.mail.polis.dao.vaddya.IteratorUtils.mergeIterators;

@ThreadSafe
final class MemTablePool implements Table, Closeable {
    private static final Logger log = LoggerFactory.getLogger(MemTablePool.class);

    private final ReadWriteLock lock;
    private volatile Table currentTable;
    private final Map<Integer, Table> pendingFlush;
    private final Flusher flusher;
    private final AtomicInteger currentGeneration;
    private final long flushThresholdInBytes;
    private final AtomicBoolean stopped;

    MemTablePool(
            @NotNull final Flusher flusher,
            final int startGeneration,
            final long flushThresholdInBytes) {
        this.lock = new ReentrantReadWriteLock();
        this.currentTable = new MemTable();
        this.pendingFlush = new TreeMap<>();
        this.flusher = flusher;
        this.currentGeneration = new AtomicInteger(startGeneration);
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.stopped = new AtomicBoolean();
    }

    @NotNull
    @Override
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        lock.readLock().lock();
        Collection<Iterator<TableEntry>> iterators;
        try {
            iterators = collectIterators(currentTable, pendingFlush.values(), from);
        } finally {
            lock.readLock().unlock();
        }
        return mergeIterators(iterators);
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
            throw new IllegalStateException("Mem table was already closed");
        }
        currentTable.upsert(key, value);
        if (currentTable.currentSize() > flushThresholdInBytes) { // to avoid extra call & lock
            enqueueToFlush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (stopped.get()) {
            throw new IllegalStateException("Mem table was already closed");
        }
        currentTable.remove(key);
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

    void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    int getAndIncrementGeneration() {
        return currentGeneration.getAndIncrement();
    }

    private void enqueueToFlush() {
        lock.writeLock().lock();
        try {
            if (currentTable.currentSize() > flushThresholdInBytes) {
                final var generation = currentGeneration.getAndIncrement();
                pendingFlush.put(generation, currentTable);
                flusher.flush(generation, currentTable);
                currentTable = new MemTable();
                log.debug("Table {} with size {}B was submitted to flush", generation, currentTable.currentSize());
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
                final var generation = currentGeneration.getAndIncrement();
                flusher.flush(generation, currentTable);
                log.debug("Table {} with size {}B was submitted to flush", generation, currentTable.currentSize());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
