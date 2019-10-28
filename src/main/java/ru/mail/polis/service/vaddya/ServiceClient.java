package ru.mail.polis.service.vaddya;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.jetbrains.annotations.NotNull;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;

import static ru.mail.polis.service.vaddya.ResponseUtils.HEADER_PROXY;

final class ServiceClient {
    private static final String PATH_ENTITY = "/v0/entity?id=";

    private final HttpClient client;
    private final ExecutorService executor;

    ServiceClient(ConnectionString conn, ExecutorService executor) {
        this.client = new HttpClient(conn);
        this.executor = executor;
    }

    Future<Response> get(@NotNull final String id) {
        return executor.submit(() -> client.get(PATH_ENTITY + id, HEADER_PROXY));
    }

    Future<Response> put(
            @NotNull final String id,
            @NotNull final byte[] data) {
        return executor.submit(() -> client.put(PATH_ENTITY + id, data, HEADER_PROXY));
    }

    Future<Response> delete(@NotNull final String id) {
        return executor.submit(() -> client.delete(PATH_ENTITY + id, HEADER_PROXY));
    }
}
