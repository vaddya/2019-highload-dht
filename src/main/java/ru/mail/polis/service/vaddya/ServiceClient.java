package ru.mail.polis.service.vaddya;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous client for {@link ru.mail.polis.service.Service}.
 */
public interface ServiceClient {
    /**
     * Get a value by key asynchronously.
     *
     * @param id key
     * @return future of value
     */
    @NotNull
    CompletableFuture<Value> getAsync(@NotNull String id);

    /**
     * Put a value by key asynchronously.
     *
     * @param id   key
     * @param data value
     * @return future of nothing
     */
    @NotNull
    CompletableFuture<Void> putAsync(
            @NotNull String id,
            @NotNull byte[] data);

    /**
     * Delete a value by key asynchronously.
     *
     * @param id key
     * @return future of nothing
     */
    @NotNull
    CompletableFuture<Void> deleteAsync(@NotNull String id);
}
