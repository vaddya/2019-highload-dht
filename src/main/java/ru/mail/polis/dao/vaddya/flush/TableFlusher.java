package ru.mail.polis.dao.vaddya.flush;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.memtable.MemTable;
import ru.mail.polis.dao.vaddya.naming.FileManager;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

@ThreadSafe
public final class TableFlusher implements Flusher {
    private static final int THREAD_COUNT = 4;
    private static final Logger log = LoggerFactory.getLogger(TableFlusher.class);

    private final FileManager fileManager;
    private final Executor executor;
    private final Collection<FlushListener> listeners = new CopyOnWriteArrayList<>();
    private final Phaser phaser = new Phaser(1); // one party for closing call

    /**
     * Creates a flusher instance that schedules and executes
     * flushes of MemTables to the disk.
     *
     * @param fileManager a file manager to create file names
     */
    public TableFlusher(@NotNull final FileManager fileManager) {
        this.fileManager = fileManager;
        final var threadFactory = new ThreadFactoryBuilder().setNameFormat("flusher-%d").build();
        this.executor = Executors.newFixedThreadPool(THREAD_COUNT, threadFactory);
    }

    @Override
    public void scheduleFlush(
            final int generation,
            @NotNull final MemTable memTable) {
        phaser.register();
        executor.execute(() -> flush(generation, memTable));
    }

    private void flush(
            final int generation,
            @NotNull final MemTable memTable) {
        try {
            final var it = memTable.iterator();
            final var ssTable = flushEntries(generation, it);
            log.info("T{} was flushed: {}", generation, ssTable);
            listeners.forEach(listener -> listener.flushed(generation, ssTable));
        } catch (IOException e) {
            log.error("Flushing error", e);
        } finally {
            phaser.arrive();
        }
    }

    @Override
    @NotNull
    public SSTable flushEntries(
            final int generation,
            @NotNull final Iterator<TableEntry> iterator) throws IOException {
        final var tempPath = fileManager.tempPathTo(generation);
        final var finalPath = fileManager.finalPathTo(generation);
        return SSTable.flushAndOpen(iterator, tempPath, finalPath);
    }

    @Override
    public void addListener(@NotNull final FlushListener flushListener) {
        listeners.add(flushListener);
    }

    @Override
    public void close() {
        phaser.arriveAndAwaitAdvance();
    }
}
