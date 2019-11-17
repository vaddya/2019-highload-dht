package ru.mail.polis.dao.vaddya.sstable;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.vaddya.IteratorUtils;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.flush.Flusher;
import ru.mail.polis.dao.vaddya.naming.FileManager;
import ru.mail.polis.dao.vaddya.naming.GenerationProvider;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.dao.vaddya.IteratorUtils.toCollapsedMergedIterator;

/**
 * SSTable pool without background compaction process.
 */
@ThreadSafe
public final class SSTablePoolImpl implements SSTablePool {
    private static final Logger log = LoggerFactory.getLogger(SSTablePoolImpl.class);

    private final FileManager fileManager;
    private final GenerationProvider generationProvider;
    private final Flusher flusher;
    private final Map<Integer, SSTable> tables;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public SSTablePoolImpl(
            @NotNull final FileManager fileManager,
            @NotNull final Flusher flusher,
            @NotNull final GenerationProvider generationProvider) {
        this.fileManager = fileManager;
        this.flusher = flusher;
        this.generationProvider = generationProvider;

        this.tables = new TreeMap<>();
        var maxGeneration = 0;
        for (final var path : fileManager.listTables()) {
            try {
                final var generation = fileManager.generationFromPath(path);
                maxGeneration = Math.max(maxGeneration, generation);
                try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                    final var table = SSTable.open(channel);
                    tables.put(generation, table);
                }
            } catch (IllegalArgumentException e) {
                log.error("Unable to parse generation from file {}: {}", path, e.getMessage());
            } catch (IOException e) {
                log.error("Unable to read table from file {}: {}", path, e.getMessage());
            }
        }

        this.generationProvider.setNextGeneration(maxGeneration + 1);
    }

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        lock.readLock().lock();
        try {
            return tables.values()
                    .stream()
                    .map(table -> table.iterator(from))
                    .collect(toCollapsedMergedIterator());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            return tables.values()
                    .stream()
                    .mapToLong(SSTable::sizeInBytes)
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int count() {
        lock.readLock().lock();
        try {
            return tables.values()
                    .stream()
                    .mapToInt(SSTable::count)
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Map<Integer, SSTable> tables() {
        return new HashMap<>(tables);
    }

    @Override
    public void addTable(
            final int generation,
            @NotNull final SSTable table) {
        lock.writeLock().lock();
        try {
            tables.put(generation, table);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void removeTable(final int generation) {
        lock.writeLock().lock();
        try {
            tables.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        final Map<Integer, SSTable> tablesToCompact;
        lock.readLock().lock();
        try {
            tablesToCompact = new TreeMap<>(tables); // that are all tables for now
        } finally {
            lock.readLock().unlock();
        }
        final var tables = tablesToCompact.values();
        final var generations = tablesToCompact.keySet();
        final var alive = IteratorUtils.aliveEntries(tables);

        final SSTable ssTable;
        final int generation;
        if (alive.hasNext()) { // if not only tombstones
            generation = generationProvider.nextGeneration();
            ssTable = flusher.flushEntries(generation, alive);
        } else {
            generation = 0;
            ssTable = null;
        }

        lock.writeLock().lock();
        try {
            generations.forEach(this::removeTable);
            if (ssTable == null) {
                log.info("SSTables were collapsed into nothing");
            } else {
                addTable(generation, ssTable);
                log.info("SSTables were compacted");
            }
        } finally {
            lock.writeLock().unlock();
        }

        generations.stream()
                .map(fileManager::finalPathTo)
                .forEach(SSTablePoolImpl::deleteCompactedFile);
    }

    private static void deleteCompactedFile(@NotNull final Path path) {
        try {
            Files.delete(path);
            log.debug("Table was removed during compaction: {}", path);
        } catch (IOException e) {
            log.error("Unable to remove file {} during compaction: {}", path, e.getMessage());
        }
    }
}
