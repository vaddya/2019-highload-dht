package ru.mail.polis.dao.vaddya.flush;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.vaddya.ByteBufferUtils;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.memtable.MemTable;
import ru.mail.polis.dao.vaddya.naming.TableNaming;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

@ThreadSafe
public final class TableFlusher implements Flusher {
    private static final int THREAD_COUNT = 8;
    private static final Logger log = LoggerFactory.getLogger(TableFlusher.class);

    private final TableNaming tableNaming;
    private final Executor executor;
    private final Collection<FlushListener> listeners = new CopyOnWriteArrayList<>();
    private final Phaser phaser = new Phaser(1); // one party for closing call

    public TableFlusher(@NotNull final TableNaming tableNaming) {
        this.tableNaming = tableNaming;
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
            final var it = memTable.iterator(ByteBufferUtils.emptyBuffer());
            final var ssTable = flushEntries(generation, it);
            listeners.forEach(listener -> listener.flushed(generation, ssTable));
            log.info("Table {} was flushed to the disk", generation);
        } catch (IOException e) {
            log.error("Flushing error: {}", e.getMessage());
        } finally {
            phaser.arrive();
        }
    }

    @Override
    @NotNull
    public SSTable flushEntries(
            final int generation,
            @NotNull final Iterator<TableEntry> iterator) throws IOException {
        final var tempPath = tableNaming.tempPathTo(generation);
        try (var channel = FileChannel.open(tempPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            SSTable.flushEntries(iterator, channel);
        }

        final var finalPath = tableNaming.finalPathTo(generation);
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

        try (var channel = FileChannel.open(finalPath, StandardOpenOption.READ)) {
            return SSTable.from(channel);
        }
    }

    @Override
    public void addListener(@NotNull FlushListener flushListener) {
        listeners.add(flushListener);
    }

    @Override
    public void close() {
        phaser.arriveAndAwaitAdvance();
    }
}
