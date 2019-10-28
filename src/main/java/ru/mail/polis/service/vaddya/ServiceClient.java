package ru.mail.polis.service.vaddya;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.jetbrains.annotations.NotNull;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.service.vaddya.ResponseUtils.HEADER_PROXY;

final class ServiceClient {
    private static final Logger log = LoggerFactory.getLogger(ServiceClient.class);
    private static final String PATH_ENTITY = "/v0/entity";

    private final HttpClient client;
    private final ExecutorService executor;

    ServiceClient(
            @NotNull final ConnectionString conn,
            @NotNull final ExecutorService executor) {
        this.client = new HttpClient(conn);
        this.executor = executor;
    }

    Future<Response> get(@NotNull final String id) {
        log.debug("Get remote entity: id={}", id);
        return executor.submit(() -> client.get(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
    }

    Future<Response> put(
            @NotNull final String id,
            @NotNull final byte[] data) {
        log.debug("Put remote entity: id={}", id);
        return executor.submit(() -> client.put(PATH_ENTITY + "?id=" + id, data, HEADER_PROXY));
    }

    Future<Response> delete(@NotNull final String id) {
        log.debug("Delete remote entity: id={}", id);
        return executor.submit(() -> client.delete(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
    }
}
