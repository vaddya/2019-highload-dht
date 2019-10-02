package ru.mail.polis.dao.vaddya;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static ru.mail.polis.dao.vaddya.ByteBufferUtils.emptyBuffer;

public class DAOImpl implements DAO {
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String FINAL_SUFFIX = ".db";
    private static final Logger LOG = LoggerFactory.getLogger(DAOImpl.class);

    private final Table memTable = new MemTable();
    private final List<Table> ssTables;
    private final File root;
    private final long flushThresholdInBytes;

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
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.ssTables = Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList())
                .stream()
                .filter(s -> s.endsWith(FINAL_SUFFIX))
                .map(this::pathTo)
                .map(this::parseTable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
    }

    @NotNull
    private Optional<Table> parseTable(@NotNull final Path path) {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return Optional.of(Table.from(channel));
        } catch (IOException e) {
            LOG.error("{}: {}", e.getMessage(), path);
            return Optional.empty();
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterators = ssTables.stream()
                .map(table -> table.iterator(from))
                .collect(toList());
        iterators.add(memTable.iterator(from));
        final var iterator = mergeIterators(iterators);
        return Iterators.transform(iterator, e -> Record.of(e.getKey(), e.getValue()));
    }

    @SuppressWarnings("UnstableApiUsage")
    private Iterator<TableEntry> mergeIterators(@NotNull final List<Iterator<TableEntry>> iterators) {
        final var merged = Iterators.mergeSorted(iterators, TableEntry.COMPARATOR);
        final var collapsed = Iters.collapseEquals(merged, TableEntry::getKey);
        return Iterators.filter(collapsed, e -> !e.hasTombstone());
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws NoSuchEntityException {
        final var iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchEntityException("Not found");
        }
    
        final var next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchEntityException("Not found");
        }
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate().asReadOnlyBuffer(), value.duplicate().asReadOnlyBuffer());
        if (memTable.currentSize() > flushThresholdInBytes) {
            flushMemTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate().asReadOnlyBuffer());
        if (memTable.currentSize() > flushThresholdInBytes) {
            flushMemTable();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.currentSize() > 0) {
            flushMemTable();
        }
    }

    @Override
    public void compact() throws IOException {
        final var iterators = ssTables.stream()
                .map(table -> table.iterator(emptyBuffer()))
                .collect(toList());
        final var path = flushEntries(mergeIterators(iterators));

        final var file = path.toFile();
        Optional.ofNullable(root.listFiles(f -> !f.equals(file)))
                .map(Arrays::asList)
                .orElse(emptyList())
                .forEach(DAOImpl::deleteCompactedFile);

        final var table = parseTable(path);
        ssTables.clear();
        table.ifPresent(ssTables::add);
    }

    private void flushMemTable() throws IOException {
        final var iterator = memTable.iterator(emptyBuffer());
        final var path = flushEntries(iterator);

        final var table = parseTable(path);
        memTable.clear();
        table.ifPresent(ssTables::add);
    }

    @NotNull
    private Path flushEntries(@NotNull final Iterator<TableEntry> iterator) throws IOException {
        final var now = LocalDateTime.now().toString();
        final var tempPath = pathTo(now + TEMP_SUFFIX);
        try (var channel = FileChannel.open(tempPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            Table.flushEntries(iterator, channel);
        }

        final var finalPath = pathTo(now + FINAL_SUFFIX);
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

        return finalPath;
    }

    @NotNull
    private Path pathTo(@NotNull final String name) {
        return Path.of(root.getAbsolutePath(), name);
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
