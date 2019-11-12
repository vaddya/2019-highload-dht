package ru.mail.polis.dao.vaddya;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;

@ThreadSafe
final class TableFlusher implements Flusher, Closeable {
    private static final int THREAD_COUNT = 8;
    
    private final DAOImpl dao;
    private final Executor executor;
    private final Phaser phaser = new Phaser(1); // one party for closing call

    TableFlusher(@NotNull final DAOImpl dao) {
        this.dao = dao;
        final var threadFactory = new ThreadFactoryBuilder().setNameFormat("flusher-%d").build();
        this.executor = Executors.newFixedThreadPool(THREAD_COUNT, threadFactory);
    }

    @Override
    public void flush(
            final int generation, 
            @NotNull final Table table) {
        phaser.register();
        executor.execute(() -> {
            try {
                dao.flushAndOpen(generation, table);
            } finally {
                phaser.arrive();
            }
        });
    }

    @Override
    public void close() {
        phaser.arriveAndAwaitAdvance();
    }
}
