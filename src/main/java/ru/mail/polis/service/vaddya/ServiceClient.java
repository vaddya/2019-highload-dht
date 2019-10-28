package ru.mail.polis.service.vaddya;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import one.nio.http.HttpException;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.service.vaddya.ResponseUtils.HEADER_PROXY;

final class ServiceClient extends HttpClient {
    private static final long serialVersionUID = -4873134095723122623L;
    private static final Logger log = LoggerFactory.getLogger(ServiceClient.class);
    private static final String PATH_ENTITY = "/v0/entity";

    @SuppressWarnings("UnusedVariable")
    private final ExecutorService executor;

    ServiceClient(
            @NotNull final ConnectionString conn,
            @NotNull final ExecutorService executor) {
        super(conn);
        this.executor = executor;
    }

    Future<Response> get(@NotNull final String id) {
//    Response get(@NotNull final String id) {
        log.debug("Get remote entity: toPort={}, id={}", port, hash(id));
        return executor.submit(() -> get(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
//        try {
//            return get(PATH_ENTITY + "?id=" + id, HEADER_PROXY);
//        } catch (InterruptedException | PoolException | IOException | HttpException e) {
//            log.debug(e.getMessage());
//            return null;
//        }
    }

    Future<Response> put(
//    Response put(
            @NotNull final String id,
            @NotNull final byte[] data) {
        log.debug("Put remote entity: toPort={}, id={}", port, hash(id));
        return executor.submit(() -> put(PATH_ENTITY + "?id=" + id, data, HEADER_PROXY));
//        try {
//            return put(PATH_ENTITY + "?id=" + id, data, HEADER_PROXY);
//        } catch (InterruptedException | PoolException | IOException | HttpException e) {
//            log.debug(e.getMessage());
//            return null;
//        }
    }

    Future<Response> delete(@NotNull final String id) {
//    Response delete(@NotNull final String id) {
        log.debug("Delete remote entity: toPort={}, id={}", port, hash(id));
        return executor.submit(() -> delete(PATH_ENTITY + "?id=" + id, HEADER_PROXY));
//        try {
//            return delete(PATH_ENTITY + "?id=" + id, HEADER_PROXY);
//        } catch (InterruptedException | PoolException | IOException | HttpException e) {
//            log.debug(e.getMessage());
//            return null;
//        }
    }

    private static int hash(@NotNull final String id) {
        return ByteBufferUtils.wrapString(id).hashCode();
    }
}
