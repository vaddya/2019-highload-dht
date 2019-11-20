package ru.mail.polis.dao.vaddya.sstable.leveled;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.naming.GenerationProvider;
import ru.mail.polis.dao.vaddya.naming.LeveledFileManagerImpl;
import ru.mail.polis.dao.vaddya.sstable.SSTable;
import ru.mail.polis.dao.vaddya.sstable.SSTablePool;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static ru.mail.polis.dao.vaddya.IteratorUtils.toCollapsedMergedIterator;

@ThreadSafe
public class LeveledSSTablePoolImpl implements SSTablePool {
    private static final int LEVELS_COUNT = 4;
    private static final Logger log = LoggerFactory.getLogger(LeveledSSTablePoolImpl.class);

    private final List<Level> levels;
    private final LeveledFileManagerImpl fileManager;
    private final long compactionThresholdInBytes;
    private final LeveledCompactor compactor = new LeveledCompactor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates a SSTablePool instance with leveled compaction the in background.
     * https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
     *
     * @param compactionThresholdInBytes threshold in bytes when zero level need to be compacted
     * @param targetTableSizeInBytes     target base size of a table on a disk
     * @param fileManager                a file manager to access files
     * @param generationProvider         a generation provider to atomically increment and get generation
     */
    public LeveledSSTablePoolImpl(
            final long compactionThresholdInBytes,
            final long targetTableSizeInBytes,
            @NotNull final LeveledFileManagerImpl fileManager,
            @NotNull final GenerationProvider generationProvider) {
        this.fileManager = fileManager;
        this.compactionThresholdInBytes = compactionThresholdInBytes;
        this.levels = IntStream.range(0, LEVELS_COUNT)
                .mapToObj(index -> new Level(index, targetTableSizeInBytes, fileManager, generationProvider))
                .collect(toUnmodifiableList());

        final var maxGeneration = openTables();
        generationProvider.setNextGeneration(maxGeneration + 1);
    }

    @NotNull
    @Override
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        lock.readLock().lock();
        try {
            return levels.stream()
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
            return levels.stream()
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
            return levels.stream()
                    .mapToInt(SSTable::count)
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    @NotNull
    public Map<Integer, SSTable> tables() {
        lock.readLock().lock();
        try {
            return levels.stream()
                    .flatMap(level -> level.tables().entrySet().stream())
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void addTable(
            final int generation,
            @NotNull final SSTable table) {
        lock.readLock().lock();
        try {
            level(0).addTable(generation, table);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void removeTable(final int generation) {
        lock.readLock().lock();
        try {
            level(0).removeTable(generation);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        compactor.compact();
    }

    @Override
    public void close() {
        compactor.close();
    }

    @Override
    public String toString() {
        return "LeveledSSTablePoolImpl{" + "levels=" + levels + '}';
    }

    @NotNull
    private Level level(final int index) {
        if (index < 0 || index >= levels.size()) {
            throw new IllegalArgumentException("Wrong level index: " + index);
        }
        return levels.get(index);
    }

    private int openTables() {
        var maxGeneration = 0;
        for (final var path : fileManager.listTables()) {
            try {
                final var generation = fileManager.generationFromPath(path);
                maxGeneration = Math.max(maxGeneration, generation);
                final var level = fileManager.levelFromPath(path);
                if (level < 0 || level >= LEVELS_COUNT) {
                    throw new IllegalArgumentException("Level == " + level);
                }
                try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                    final var ssTable = SSTable.open(channel);
                    levels.get(level).addTable(generation, ssTable);
                }
            } catch (IllegalArgumentException e) {
                log.error("Unable to parse generation or level from file name {}: {}", path, e.getMessage());
            } catch (IOException e) {
                log.error("Unable to read table from file {}: {}", path, e.getMessage());
            }
        }
        return maxGeneration;
    }

    private class LeveledCompactor implements Closeable {
        // check every FREQUENCY seconds if compaction needs to be done
        private static final int FREQUENCY = 1;
        private static final int TIMEOUT = 60;

        private final ScheduledExecutorService executor;
        private final ScheduledFuture<?> future;
        private final AtomicBoolean isCompacting = new AtomicBoolean();
        private CountDownLatch compactionLatch = new CountDownLatch(1);

        LeveledCompactor() {
            final var threadFactory = new ThreadFactoryBuilder().setNameFormat("compactor-%d").build();
            this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
            this.future = executor.scheduleAtFixedRate(this::compactIfNeeded, FREQUENCY, FREQUENCY, TimeUnit.SECONDS);
        }

        void compact() throws IOException {
            if (!isCompacting.compareAndSet(false, true)) {
                try {
                    compactionLatch.await(TIMEOUT, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.error("Error while awaiting compaction completion: {}", e.getMessage());
                    Thread.currentThread().interrupt();
                }
                return;
            }
            try {
                compactionLatch = new CountDownLatch(1);
                for (int i = 0; i < LEVELS_COUNT - 1; i++) {
                    if (!compactLevel(i)) {
                        break;
                    }
                }
                log.info("After compaction: {}", levels);
            } finally {
                isCompacting.set(false);
                compactionLatch.countDown();
            }
        }

        @Override
        public void close() {
            executor.shutdown();
            if (isCompacting.get() && !future.isDone()) {
                try {
                    future.cancel(false);
                    if (!executor.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
                        log.error("Unable to await termination of compactor");
                    }
                } catch (InterruptedException e) {
                    log.error("Unable to stop compactor {}", e.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
        }

        private boolean compactLevel(final int index) throws IOException {
            if (index == 0) {
                return compactZeroLevel();
            }

            final var level = level(index);
            if (level.sizeInBytes() < getLevelCompactionThreshold(index)) {
                return false;
            }

            final var lowerTable = level(index).maxSizedTable();
            final var higherLevelSnapshot = level(index + 1).snapshot();
            log.info("Merging T{} of {} with {} tables of L{}", lowerTable, index, higherLevelSnapshot.tables().size(), index + 1);

            final var createdTables = higherLevelSnapshot.mergeWith(lowerTable.getValue());

            lock.writeLock().lock();
            try {
                level(index).removeTable(lowerTable.getKey());
                level(index + 1).removeTables(higherLevelSnapshot.tables().keySet());
                level(index + 1).addTables(createdTables);
            } finally {
                lock.writeLock().unlock();
            }
            return true;
        }

        private boolean compactZeroLevel() throws IOException {
            final var zeroLevelSnapshot = level(0).snapshot();
            final var firstLevelSnapshot = level(1).snapshot();
            log.info("Merging {} tables of L0 with {} tables of L1", zeroLevelSnapshot.tables().size(), firstLevelSnapshot.tables().size());
            final var createdTables = firstLevelSnapshot.mergeWith(zeroLevelSnapshot);

            lock.writeLock().lock();
            try {
                level(0).removeTables(zeroLevelSnapshot.tables().keySet());
                level(1).removeTables(firstLevelSnapshot.tables().keySet());
                level(1).addTables(createdTables);
            } finally {
                lock.writeLock().unlock();
            }
            return true;
        }

        private void compactIfNeeded() {
            if (isCompacting.get()) {
                return;
            }

            final long zeroLevelSizeInBytes;
            lock.readLock().lock();
            try {
                zeroLevelSizeInBytes = level(0).sizeInBytes();
            } finally {
                lock.readLock().unlock();
            }

            if (zeroLevelSizeInBytes > compactionThresholdInBytes) {
                try {
                    compact();
                } catch (IOException e) {
                    log.error("Unable to compact: {}", e.getMessage());
                } catch (RejectedExecutionException e) {
                    log.error("Unable to submit compaction task", e);
                }
            } else {
                log.debug("No compaction needed: {}/{}", zeroLevelSizeInBytes, compactionThresholdInBytes);
            }
        }

        private long getLevelCompactionThreshold(final int index) {
            return compactionThresholdInBytes * (index + 1);
        }
    }
}
