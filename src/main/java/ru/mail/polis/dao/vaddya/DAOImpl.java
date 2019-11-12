package ru.mail.polis.dao.vaddya;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static ru.mail.polis.dao.vaddya.IteratorUtils.collapseIterators;
import static ru.mail.polis.dao.vaddya.IteratorUtils.collectIterators;

@ThreadSafe
public class DAOImpl implements DAO {
    private static final String FINAL_SUFFIX = ".db";
    private static final String TEMP_SUFFIX = ".tmp";
    private static final Logger log = LoggerFactory.getLogger(DAOImpl.class);

    private final File root;
    private final MemTablePool memTablePool;
    private final Map<Integer, Table> ssTables = new HashMap<>();
    private final TableFlusher flusher = new TableFlusher(this);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates persistent DAO.
     *
     * @param root folder to save and read data from
     * @throws UncheckedIOException if cannot open or read SSTables
     */
    public DAOImpl(
            @NotNull final File root,
            final long flushThresholdInBytes) {
        this.root = root;
        final var maxGeneration = openDirectory(root);
        this.memTablePool = new MemTablePool(flusher, maxGeneration + 1, flushThresholdInBytes);
        log.info("DAO was opened in directory {}, SSTables count={}", root, ssTables.size());
    }

    @Override
    @NotNull
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterator = entryIterator(from);
        final var alive = Iterators.filter(iterator, e -> !e.hasTombstone());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue()));
    }

    /**
     * Get an entry iterator.
     * Returning value by iterator could be a tombstone.
     *
     * @param from starting key to search for
     * @return an iterator
     */
    @NotNull
    public Iterator<TableEntry> entryIterator(@NotNull final ByteBuffer from) {
        final Collection<Iterator<TableEntry>> iterators;
        lock.readLock().lock();
        try {
            iterators = collectIterators(memTablePool, ssTables.values(), from);
        } finally {
            lock.readLock().unlock();
        }
        return collapseIterators(iterators);
    }

    @Override
    @NotNull
    public ByteBuffer get(@NotNull final ByteBuffer key) throws NoSuchEntityException {
        final var iterator = iterator(key);
        if (!iterator.hasNext()) {
            throw new NoSuchEntityException("Not found");
        }

        final var next = iterator.next();
        if (!next.getKey().equals(key)) {
            throw new NoSuchEntityException("Not found");
        }

        return next.getValue();
    }

    /**
     * Get an entry for a given key.
     * Value could be a tombstone.
     *
     * @param key key to search for
     * @return value or {@code null}
     */
    @Nullable
    public TableEntry getEntry(@NotNull final ByteBuffer key) {
        final var iterator = entryIterator(key);
        if (!iterator.hasNext()) {
            return null;
        }

        final var next = iterator.next();
        if (!next.getKey().equals(key)) {
            return null;
        }

        return next;
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        memTablePool.upsert(key.duplicate().asReadOnlyBuffer(), value.duplicate().asReadOnlyBuffer());
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        memTablePool.remove(key.duplicate().asReadOnlyBuffer());
    }

    @Override
    public void close() {
        memTablePool.close();
        flusher.close();
    }

    @Override
    public void compact() throws IOException {
        final var tablesToCompact = getTablesToCompact();
        final var tables = tablesToCompact.values();
        final var generations = tablesToCompact.keySet();

        final var iterators = tables.stream()
                .map(table -> table.iterator(ByteBufferUtils.emptyBuffer()))
                .collect(toList());
        final var iterator = collapseIterators(iterators);
        final var alive = Iterators.filter(iterator, e -> !e.hasTombstone());

        final Table compactedTable;
        final int generation;
        final Path path;
        if (alive.hasNext()) { // if not only tombstones
            generation = memTablePool.getAndIncrementGeneration();
            path = flushEntries(generation, alive);
            compactedTable = parseTable(path);
        } else {
            generation = 0;
            path = null;
            compactedTable = null;
        }

        lock.writeLock().lock();
        try {
            generations.forEach(ssTables::remove);
            if (compactedTable == null) {
                log.info("SSTables were collapsed into nothing");
            } else {
                ssTables.put(generation, compactedTable);
                log.info("SSTables were compacted into {}", path);
            }
        } finally {
            lock.writeLock().unlock();
        }

        generations.stream()
                .map(this::pathToGeneration)
                .forEach(DAOImpl::deleteCompactedFile);
    }

    private int openDirectory(@NotNull final File root) {
        final var tableFiles = Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList())
                .stream()
                .filter(s -> s.endsWith(FINAL_SUFFIX))
                .collect(toList());
        var maxGeneration = 0;
        for (final var name : tableFiles) {
            try {
                final var generation = parseGeneration(name);
                maxGeneration = Math.max(generation, maxGeneration);

                final var path = Path.of(root.getAbsolutePath(), name);
                final var table = parseTable(path);
                this.ssTables.put(generation, table);
            } catch (IllegalArgumentException e) {
                log.error("Unable to parse generation from file {}: {}", name, e.getMessage());
            } catch (IOException e) {
                log.error("Unable to read table from file {}: {}", name, e.getMessage());
            }
        }
        return maxGeneration;
    }
    
    @NotNull
    private Map<Integer, Table> getTablesToCompact() {
        lock.readLock().lock();
        try {
            return new HashMap<>(ssTables); // NB: that are all SSTables for now
        } finally {
            lock.readLock().unlock();
        }
    }

    void flushAndOpen(
            final int generation,
            @NotNull final Table table) {
        try {
            final var it = table.iterator(ByteBufferUtils.emptyBuffer());
            final var path = flushEntries(generation, it);
            final var ssTable = parseTable(path);

            lock.writeLock().lock();
            try {
                memTablePool.flushed(generation);
                ssTables.put(generation, ssTable);
            } finally {
                lock.writeLock().unlock();
            }

            log.info("Table {} was flushed to the disk into {}", generation, path);
        } catch (IOException e) {
            log.error("Flushing error: {}", e.getMessage());
        }
    }

    @NotNull
    private Path flushEntries(
            final int generation,
            @NotNull final Iterator<TableEntry> iterator) throws IOException {
        final var tempPath = pathTo(generation + TEMP_SUFFIX);
        try (var channel = FileChannel.open(tempPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            Table.flushEntries(iterator, channel);
        }

        final var finalPath = pathTo(generation + FINAL_SUFFIX);
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

        return finalPath;
    }

    @NotNull
    private Path pathToGeneration(final int generation) {
        return pathTo(generation + FINAL_SUFFIX);
    }

    @NotNull
    private Path pathTo(@NotNull final String name) {
        return Path.of(root.getAbsolutePath(), name);
    }

    @NotNull
    private static Table parseTable(@NotNull final Path path) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return Table.from(channel);
        }
    }

    private static int parseGeneration(@NotNull final String name) throws IllegalArgumentException {
        if (name.length() <= FINAL_SUFFIX.length()) {
            throw new IllegalArgumentException("File name is too short");
        }
        final var substring = name.substring(0, name.length() - FINAL_SUFFIX.length());
        return Integer.parseInt(substring);
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
