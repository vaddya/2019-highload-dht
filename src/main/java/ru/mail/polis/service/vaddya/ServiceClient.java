package ru.mail.polis.service.vaddya;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Future;

/**
 * HTTP client for {@link ru.mail.polis.service.Service}.
 */
public interface ServiceClient {
    /**
     * Get a value by key.
     *
     * @param id key
     * @return HTTP response
     */
    @NotNull
    Future<Response> get(@NotNull String id);

    /**
     * Put a value by key.
     *
     * @param id   key
     * @param data value
     * @return HTTP response
     */
    @NotNull
    Future<Response> put(
            @NotNull String id,
            @NotNull byte[] data);

    /**
     * Delete a value by key.
     *
     * @param id key
     * @return HTTP response
     */
    @NotNull
    Future<Response> delete(@NotNull String id);
}
