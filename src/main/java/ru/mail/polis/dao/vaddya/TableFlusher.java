package ru.mail.polis.dao.vaddya;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import org.jetbrains.annotations.NotNull;

@ThreadSafe
final class TableFlusher implements Flusher, Closeable {
    private static final int THREAD_COUNT = 4;
    
    private final DAOImpl dao;
    private final Executor executor;
    private final Phaser phaser;

    TableFlusher(@NotNull final DAOImpl dao) {
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(THREAD_COUNT);
        this.phaser = new Phaser(1); // one party for close call  
    }

    @Override
    public void flush(
            final int generation, 
            @NotNull final Table table) {
        final var task = new FlushTask(generation, table);
        phaser.register();
        executor.execute(task);
    }

    @Override
    public void close() {
        phaser.arriveAndAwaitAdvance();
    }

    private class FlushTask implements Runnable {
        private final int generation;
        private final Table table;

        FlushTask(
                final int generation,
                @NotNull final Table table) {
            this.generation = generation;
            this.table = table;
        }

        @Override
        public void run() {
            final var it = table.iterator(ByteBufferUtils.emptyBuffer());
            dao.flushAndOpen(generation, it);
            phaser.arrive();
        }
    }
}
