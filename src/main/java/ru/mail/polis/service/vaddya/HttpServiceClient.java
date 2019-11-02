package ru.mail.polis.service.vaddya;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static ru.mail.polis.service.vaddya.ResponseUtils.HEADER_PROXY;

final class HttpServiceClient extends HttpClient implements ServiceClient {
    private static final long serialVersionUID = -4873134095723122623L;
    private static final Logger log = LoggerFactory.getLogger(HttpServiceClient.class);
    private static final String PATH_ENTITY = "/v0/entity";
    private static final int TIMEOUT = 100;

    private final ExecutorService executor;

    HttpServiceClient(
            @NotNull final String node,
            @NotNull final ExecutorService executor) {
        super(new ConnectionString(node + "?timeout=" + TIMEOUT));
        this.executor = executor;
    }

    @Override
    @NotNull
    public Future<Response> get(@NotNull final String id) {
        log.debug("Get remote entity: toPort={}, id={}", port, id.hashCode());
        return executor.submit(() -> get(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
    }

    @Override
    @NotNull
    public Future<Response> put(
            @NotNull final String id,
            @NotNull final byte[] data) {
        log.debug("Put remote entity: toPort={}, id={}", port, id.hashCode());
        return executor.submit(() -> put(PATH_ENTITY + "?id=" + id, data, HEADER_PROXY));
    }

    @Override
    @NotNull
    public Future<Response> delete(@NotNull final String id) {
        log.debug("Delete remote entity: toPort={}, id={}", port, id.hashCode());
        return executor.submit(() -> delete(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
    }
}