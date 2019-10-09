package ru.mail.polis.dao.vaddya;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

public class DAOImpl implements DAO {
    private static final Logger LOG = LoggerFactory.getLogger(DAOImpl.class);
    private static final String FINAL_SUFFIX = ".db";
    private static final String TEMP_SUFFIX = ".tmp";

    private final File root;
    private final TableFlusher flusher;
    private final MemTablePool memTablePool;
    private final List<Table> ssTables;
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
        this.ssTables = new ArrayList<>();

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
                this.ssTables.add(table);
            } catch (IllegalArgumentException e) {
                LOG.error("Unable to parse generation from file {}", name, e);
            } catch (IOException e) {
                LOG.error("Unable to read table from file {}", name, e);
            }
        }

        this.memTablePool = new MemTablePool(flusher, maxGeneration, flushThresholdInBytes);
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    @NotNull
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        Collection<Iterator<TableEntry>> iterators;
        lock.readLock().lock();
        try {
            iterators = collectIterators(memTablePool, ssTables, from);
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
        final var iterators = ssTables.stream()
                .map(table -> table.iterator(ByteBufferUtils.emptyBuffer()))
                .collect(toList());
        final var it = mergeIterators(iterators);
        final var path = flushEntries(0, it);
        final var file = path.toFile();

        lock.writeLock().lock();
        try {
            Optional.ofNullable(root.listFiles(f -> !f.equals(file)))
                    .map(Arrays::asList)
                    .orElse(emptyList())
                    .forEach(DAOImpl::deleteCompactedFile);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void flushAndOpen(
            final int generation,
            @NotNull final Iterator<TableEntry> iterator) {
        try {
            final var path = flushEntries(generation, iterator);
            final var table = parseTable(path);

            lock.writeLock().lock();
            try {
                memTablePool.flushed(generation);
                ssTables.add(table);
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            LOG.error("Flushing error", e);
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

    private static void deleteCompactedFile(@NotNull final File file) {
        try {
            Files.delete(file.toPath());
            LOG.trace("Table is removed during compaction: {}", file);
        } catch (IOException e) {
            LOG.error("Unable to remove file {} during compaction: {}", file, e.getMessage());
        }
    }
}
