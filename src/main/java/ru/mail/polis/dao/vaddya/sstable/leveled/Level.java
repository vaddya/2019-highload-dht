package ru.mail.polis.dao.vaddya.sstable.leveled;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.vaddya.ByteBufferUtils;
import ru.mail.polis.dao.vaddya.IteratorUtils;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.naming.GenerationProvider;
import ru.mail.polis.dao.vaddya.naming.LeveledFileManagerImpl;
import ru.mail.polis.dao.vaddya.sstable.SSTable;
import ru.mail.polis.dao.vaddya.sstable.SSTablePool;
import ru.mail.polis.dao.vaddya.sstable.leveled.LevelUtils.RangedSSTable;
import ru.mail.polis.dao.vaddya.sstable.leveled.LevelUtils.SingleValueTable;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
final class Level implements SSTablePool {
    private static final Logger log = LoggerFactory.getLogger(Level.class);

    private final NavigableSet<RangedSSTable> tables = new TreeSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final int index;
    private final long targetTableSizeInBytes;
    private final LeveledFileManagerImpl fileManager;
    private final GenerationProvider generationProvider;

    Level(
            final int index,
            final long targetTableSizeInBytes,
            @NotNull final LeveledFileManagerImpl fileManager,
            @NotNull final GenerationProvider generationProvider) {
        this.index = index;
        this.targetTableSizeInBytes = targetTableSizeInBytes;
        this.fileManager = fileManager;
        this.generationProvider = generationProvider;
    }

    @Override
    public void addTable(
            final int generation,
            @NotNull final SSTable table) {
        lock.writeLock().lock();
        try {
            tables.add(RangedSSTable.from(generation, table));
            log.debug("T{} is added to L{}", generation, index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void addTables(@NotNull final Map<Integer, SSTable> tables) {
        lock.writeLock().lock();
        try {
            tables.entrySet()
                    .stream()
                    .map(e -> RangedSSTable.from(e.getKey(), e.getValue()))
                    .forEach(this.tables::add);
            log.debug("Tables {} are added to L{}", tables.keySet(), index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void removeTable(final int generation) {
        lock.writeLock().lock();
        try {
            tables.remove(RangedSSTable.fromGeneration(generation));
            log.debug("T{} is removed from L{}", generation, index);
        } finally {
            lock.writeLock().unlock();
        }

        removeFile(generation);
    }

    void removeTables(@NotNull final Collection<Integer> generations) {
        lock.writeLock().lock();
        try {
            generations.stream()
                    .map(RangedSSTable::fromGeneration)
                    .forEach(tables::remove);
            log.debug("Tables {} are removed from L{}", generations, index);
        } finally {
            lock.writeLock().unlock();
        }

        generations.forEach(this::removeFile);
    }

    @Override
    public void compact() {
        // do nothing
    }

    Map.Entry<Integer, SSTable> maxSizedTable() {
        lock.readLock().lock();
        try {
            return tables.stream()
                    .max(Comparator.comparing(x -> x.ssTable.sizeInBytes()))
                    .map(x -> Map.entry(x.generation, x.ssTable))
                    .orElseThrow(() -> new IllegalStateException("Should never happen"));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Merge tables of the current level with the specified table.
     *
     * @param table table (or level) to be merged with
     * @return map of created SSTables with their generation.
     * @throws IOException if any IO error was occurred
     */
    Map<Integer, SSTable> mergeWith(@NotNull final SSTable table) throws IOException {
        final var totalEntriesCount = table.count() + count();
        final var totalSizeInBytes = table.sizeInBytes() + sizeInBytes();
        final var tableCount = Math.max(totalSizeInBytes / getTargetTableSizeInBytes(), 1);
        final var entriesPerTable = (int) (totalEntriesCount / tableCount);

        return tables.isEmpty()
                ? flushWithoutMerge(table, entriesPerTable)
                : flushWithMerge(table, entriesPerTable);
    }

    @NotNull
    private Map<Integer, SSTable> flushWithoutMerge(
            @NotNull final SSTable table,
            final int entriesPerTable) throws IOException {
        final var result = new HashMap<Integer, SSTable>();
        final var iterator = table.iterator();
        flushEntries(iterator, entriesPerTable, result);
        return result;
    }

    @NotNull
    private Map<Integer, SSTable> flushWithMerge(
            final @NotNull SSTable table,
            final int entriesPerTable) throws IOException {
        final var result = new HashMap<Integer, SSTable>();

        // flush entries with keys lower than keys of the current level tables
        final var levelLowest = lowest();
        final var levelHighest = highest();
        final var lower = table.range(ByteBufferUtils.emptyBuffer(), levelLowest);
        flushEntries(lower, entriesPerTable, result);

        // merge with tables of the current level
        final var range = table.range(levelLowest, levelHighest);
        final var merged = IteratorUtils.collapseIterators(Set.of(iterator(), range));
        flushEntries(merged, entriesPerTable, result);

        // flush entries with keys higher than keys of the current level tables
        final var currentLevelHighest = highest();
        final var higher = table.iterator(currentLevelHighest);
        flushEntries(higher, entriesPerTable, result);

        return result;
    }

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        final Collection<RangedSSTable> ssTables;
        lock.readLock().lock();
        try {
            if (index == 0) {
                ssTables = new TreeSet<>(tables);
            } else {
                ssTables = tables.tailSet(RangedSSTable.fromValue(from));
            }
        } finally {
            lock.readLock().unlock();
        }

        final var iterators = ssTables.stream()
                .map(table -> table.ssTable.iterator(from))
                .collect(toList());
        return IteratorUtils.collapseIterators(iterators);
    }

    @NotNull
    @Override
    public Iterator<TableEntry> range(
            @NotNull final ByteBuffer from,
            @Nullable final ByteBuffer to) {
        if (to == null) {
            return iterator(from);
        }

        final Collection<RangedSSTable> ssTables;
        lock.readLock().lock();
        try {
            if (index == 0) {
                ssTables = new TreeSet<>(tables);
            } else {
                ssTables = tables.subSet(RangedSSTable.fromValue(from), RangedSSTable.fromValue(to));
            }
        } finally {
            lock.readLock().unlock();
        }

        final var iterators = ssTables.stream()
                .map(table -> table.ssTable.range(from, to))
                .collect(toList());
        return IteratorUtils.collapseIterators(iterators);
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            return tables.stream()
                    .mapToLong(x -> x.ssTable.sizeInBytes())
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int count() {
        lock.readLock().lock();
        try {
            return tables.stream()
                    .mapToInt(x -> x.ssTable.count())
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Map<Integer, SSTable> tables() {
        lock.readLock().lock();
        try {
            return tables.stream()
                    .collect(toMap(x -> x.generation, x -> x.ssTable));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a snapshot of the current level that contains all tables of this level.
     *
     * @return a level snapshot
     */
    Level snapshot() {
        lock.readLock().lock();
        try {
            final var level = new Level(index, targetTableSizeInBytes, fileManager, generationProvider);
            level.tables.addAll(tables);
            return level;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "L" + index + " (" + tables.size() + " tables): " + tables;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void flushEntries(
            @NotNull final Iterator<TableEntry> iterator,
            final int entriesPerTable,
            @NotNull final Map<Integer, SSTable> result) throws IOException {
        while (iterator.hasNext()) {
            final var generation = generationProvider.nextGeneration();
            final var limited = Iterators.limit(iterator, entriesPerTable);
            final var ssTable = flushAndOpen(generation, limited);
            result.put(generation, ssTable);
            log.info("Entries merged with L{} into T{}: {}", index, generation, ssTable);
        }
    }
    
    @NotNull
    private SSTable flushAndOpen(
            final int generation,
            @NotNull final Iterator<TableEntry> iterator) throws IOException {
        final var tempPath = fileManager.tempPathTo(generation, index);
        final var finalPath = fileManager.finalPathTo(generation, index);
        return SSTable.flushAndOpen(iterator, tempPath, finalPath);
    }

    private long getTargetTableSizeInBytes() {
        return targetTableSizeInBytes * (index + 1);
    }

    private void removeFile(final int generation) {
        final var path = fileManager.finalPathTo(generation, index);
        try {
            Files.delete(path);
            log.debug("File removed: {}", path);
        } catch (IOException e) {
            log.error("Unable to remove T{}@L{}: {}", generation, index, e.getMessage());
        }
    }
}
