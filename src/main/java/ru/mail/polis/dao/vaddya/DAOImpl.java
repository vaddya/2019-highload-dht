package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.vaddya.flush.Flusher;
import ru.mail.polis.dao.vaddya.flush.TableFlusher;
import ru.mail.polis.dao.vaddya.memtable.MemTablePool;
import ru.mail.polis.dao.vaddya.memtable.MemTablePoolImpl;
import ru.mail.polis.dao.vaddya.naming.AtomicGenerationProvider;
import ru.mail.polis.dao.vaddya.naming.LeveledFileManagerImpl;
import ru.mail.polis.dao.vaddya.sstable.leveled.LeveledSSTablePoolImpl;
import ru.mail.polis.dao.vaddya.sstable.SSTable;
import ru.mail.polis.dao.vaddya.sstable.SSTablePool;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class DAOImpl implements DAO {
    private static final Logger log = LoggerFactory.getLogger(DAOImpl.class);

    private final SSTablePool ssTablePool;
    private final MemTablePool memTablePool;
    private final Flusher flusher;
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
        final var fileManager = new LeveledFileManagerImpl(root);
        final var generationProvider = new AtomicGenerationProvider();
        
        this.flusher = new TableFlusher(fileManager);
        this.flusher.addListener(this::flushed);
        this.memTablePool = new MemTablePoolImpl(flushThresholdInBytes, generationProvider, flusher);

        final var compactionThresholdInBytes = 4 * flushThresholdInBytes;
        final var targetTableSizeInBytes = 2 * flushThresholdInBytes;
        this.ssTablePool = new LeveledSSTablePoolImpl(compactionThresholdInBytes, targetTableSizeInBytes, fileManager, 
                generationProvider);

        log.info("DAO was opened in directory {}, SSTablePool: {}", root, ssTablePool);
    }

    @Override
    @NotNull
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterator = entryIterator(from);
        final var alive = IteratorUtils.aliveEntries(iterator);
        return IteratorUtils.toRecords(alive);
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
            iterators = Set.of(
                    memTablePool.iterator(from),
                    ssTablePool.iterator(from)
            );
        } finally {
            lock.readLock().unlock();
        }
        return IteratorUtils.collapseIterators(iterators);
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
        try {
            memTablePool.close();
            ssTablePool.close();
            flusher.close();
        } catch (IOException e) {
            log.error("Error while closing DAO: {}", e.getMessage());
        }
    }

    @Override
    public void compact() throws IOException {
        ssTablePool.compact();
    }

    /**
     * Atomically remove table from the {@link MemTablePool}
     * and add to the {@link SSTablePool}.
     *
     * @param generation generation of the flushed table
     * @param ssTable    flushed table
     */
    private void flushed(
            final int generation,
            @NotNull final SSTable ssTable) {
        lock.writeLock().lock();
        try {
            memTablePool.flushed(generation);
            ssTablePool.addTable(generation, ssTable);
        } catch (IOException e) {
            log.error("Flushed error: {}", e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
