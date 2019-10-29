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

final class ServiceClient extends HttpClient {
    private static final long serialVersionUID = -4873134095723122623L;
    private static final Logger log = LoggerFactory.getLogger(ServiceClient.class);
    private static final String PATH_ENTITY = "/v0/entity";

    private final ExecutorService executor;

    ServiceClient(
            @NotNull final ConnectionString conn,
            @NotNull final ExecutorService executor) {
        super(conn);
        this.executor = executor;
    }

    Future<Response> get(@NotNull final String id) {
        if (log.isDebugEnabled()) {
            log.debug("Get remote entity: toPort={}, id={}", port, hash(id));
        }
        return executor.submit(() -> get(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
    }

    Future<Response> put(
            @NotNull final String id,
            @NotNull final byte[] data) {
        if (log.isDebugEnabled()) {
            log.debug("Put remote entity: toPort={}, id={}", port, hash(id));
        }
        return executor.submit(() -> put(PATH_ENTITY + "?id=" + id, data, HEADER_PROXY));
    }

    Future<Response> delete(@NotNull final String id) {
        if (log.isDebugEnabled()) {
            log.debug("Delete remote entity: toPort={}, id={}", port, hash(id));
        }
        return executor.submit(() -> delete(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
    }

    private static int hash(@NotNull final String id) {
        return ByteBufferUtils.wrapString(id).hashCode();
    }
}
