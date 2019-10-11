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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static ru.mail.polis.dao.vaddya.IteratorUtils.collectIterators;
import static ru.mail.polis.dao.vaddya.IteratorUtils.mergeIterators;

@ThreadSafe
public class DAOImpl implements DAO {
    private static final String FINAL_SUFFIX = ".db";
    private static final String TEMP_SUFFIX = ".tmp";
    private static final Logger log = LoggerFactory.getLogger(DAOImpl.class);

    private final File root;
    private final TableFlusher flusher;
    private final MemTablePool memTablePool;
    private final Map<Integer, Table> ssTables;
    private final ReadWriteLock lock;

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
        this.flusher = new TableFlusher(this);
        this.ssTables = new HashMap<>();

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
                log.error("Unable to parse generation from file {}", name, e);
            } catch (IOException e) {
                log.error("Unable to read table from file {}", name, e);
            }
        }

        this.memTablePool = new MemTablePool(flusher, maxGeneration + 1, flushThresholdInBytes);
        this.lock = new ReentrantReadWriteLock();

        log.debug("DAO was opened in directory {}, SSTables count: {}", root, ssTables.size());
    }

    @Override
    @NotNull
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        Collection<Iterator<TableEntry>> iterators;
        lock.readLock().lock();
        try {
            iterators = collectIterators(memTablePool, ssTables.values(), from);
        } finally {
            lock.readLock().unlock();
        }
        final var it = mergeIterators(iterators);
        return Iterators.transform(it, e -> Record.of(e.getKey(), e.getValue()));
    }

    @Override
    @NotNull
    public ByteBuffer get(@NotNull final ByteBuffer key) throws NoSuchEntityException {
        final var it = iterator(key);
        if (!it.hasNext()) {
            throw new NoSuchEntityException("Not found");
        }

        final var next = it.next();
        if (!next.getKey().equals(key)) {
            throw new NoSuchEntityException("Not found");
        }

        return next.getValue();
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
        final var iterator = mergeIterators(iterators);

        final Table compactedTable;
        final int generation;
        final Path path;
        if (iterator.hasNext()) { // if not only tombstones
            generation = memTablePool.getAndIncrementGeneration();
            path = flushEntries(generation, iterator);
            compactedTable = parseTable(path);
        } else {
            generation = 0;
            path = null;
            compactedTable = null;
        }

        lock.writeLock().lock();
        try {
            generations.stream()
                    .peek(ssTables::remove)
                    .map(this::pathToGeneration)
                    .forEach(DAOImpl::deleteCompactedFile);
            if (compactedTable != null) {
                this.ssTables.put(generation, compactedTable);
                log.debug("SSTables were compacted into {}", path);
            } else {
                log.debug("SSTables were collapsed into nothing");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @NotNull
    private Map<Integer, Table> getTablesToCompact() {
        lock.readLock().lock();
        try {
            return new HashMap<>(ssTables); // for now it's all SSTables
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

            log.debug("Table {} was flushed to the disk into {}", generation, path);
        } catch (IOException e) {
            log.error("Flushing error", e);
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
            log.trace("Table was removed during compaction: {}", path);
        } catch (IOException e) {
            log.error("Unable to remove file {} during compaction: {}", path, e.getMessage());
        }
    }
}
