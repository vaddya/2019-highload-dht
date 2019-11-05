package ru.mail.polis.service.vaddya;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

final class CompletableFutureUtils {

    private CompletableFutureUtils() {
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    static <T> CompletableFuture<Collection<T>> firstN(
            @NotNull final Collection<CompletableFuture<T>> futures,
            final int n) {
        final var maxFail = futures.size() - n;
        if (maxFail < 0) {
            throw new IllegalArgumentException("Number of requested futures is too big: " + n);
        }

        final var results = new ArrayList<T>();
        final var errors = new ArrayList<Throwable>();
        final var lock = new ReentrantLock();

        final var future = new CompletableFuture<Collection<T>>();
        final BiConsumer<T, Throwable> onComplete = (result, error) -> {
            if (future.isDone()) {
                return;
            }
            lock.lock();
            try {
                if (error != null) {
                    errors.add(error);
                    if (errors.size() > maxFail) {
                        final var failure = new NotEnoughReplicasException(errors);
                        future.completeExceptionally(failure);
                    }
                    return;
                }
                if (results.size() >= n) {
                    return;
                }
                results.add(result);
                if (results.size() == n) {
                    future.complete(results);
                }
            } finally {
                lock.unlock();
            }
        };

        futures.forEach(f -> f.whenComplete(onComplete));
        return future;
    }
}
