package ru.mail.polis.service.vaddya;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

import com.google.common.base.Charsets;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;

import static ru.mail.polis.service.vaddya.ByteBufferUtils.unwrapBytes;
import static ru.mail.polis.service.vaddya.ByteBufferUtils.wrapString;

public class ServiceImpl extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(ServiceImpl.class);

    @FunctionalInterface
    private interface ResponseSupplier {
        Response supply() throws IOException;
    }

    private final DAO dao;

    public ServiceImpl(
            final int port,
            final int workersCount,
            @NotNull final DAO dao) throws IOException {
        super(createConfig(port, workersCount));
        this.dao = dao;
    }

    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        sendEmptyResponse(session, Response.BAD_REQUEST);
    }

    @Override
    public HttpSession createSession(Socket socket) {
        return new StreamingSession(socket, this);
    }

    /**
     * Process heartbeat request and respond with empty OK response.
     */
    @Path("/v0/status")
    public Response status() {
        return emptyResponse(Response.OK);
    }

    /**
     * Process request to get, put or delete an entity by ID
     * and write response to the session instance.
     *
     * @param id      entity ID
     * @param request HTTP request
     * @param session HTTP session
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            final Request request,
            final HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendEmptyResponse(session, Response.BAD_REQUEST);
            return;
        }
        final var key = wrapString(id);
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    asyncExecute(session, () -> getEntity(key));
                    break;
                case Request.METHOD_PUT:
                    asyncExecute(session, () -> putEntity(key, request.getBody()));
                    break;
                case Request.METHOD_DELETE:
                    asyncExecute(session, () -> deleteEntity(key));
                    break;
                default:
                    log.warn("Not supported HTTP-method: " + request.getMethod());
                    sendEmptyResponse(session, Response.METHOD_NOT_ALLOWED);
            }
        } catch (Exception e) {
            log.error("Exception during request handling for id=" + id, e);
            sendEmptyResponse(session, Response.INTERNAL_ERROR);
        }
    }

    /**
     * Process request to get range of values.
     *
     * @param start   Start key
     * @param end     End key
     * @param request HTTP request
     * @param session HTTP session
     */
    @Path("/v0/entities")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            final Request request,
            final HttpSession session) {
        if (start == null || start.isEmpty() || (end != null && end.isEmpty())) {
            sendEmptyResponse(session, Response.BAD_REQUEST);
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            sendEmptyResponse(session, Response.METHOD_NOT_ALLOWED);
            return;
        }
        final var startBuffer = wrapString(start);
        final var endBuffer = end != null ? wrapString(end) : null;
        try {
            final var range = dao.range(startBuffer, endBuffer);
            ((StreamingSession) session).streamRange(range);
        } catch (IOException e) {
            log.error("Unable to get range of values", e);
        }
    }

    @NotNull
    private Response getEntity(@NotNull final ByteBuffer key) throws IOException {
        try {
            final var value = dao.get(key);
            return new Response(Response.OK, unwrapBytes(value));
        } catch (NoSuchElementException e) {
            return emptyResponse(Response.NOT_FOUND);
        }
    }

    @NotNull
    private Response putEntity(
            @NotNull final ByteBuffer key,
            @Nullable final byte[] bytes) throws IOException {
        if (bytes == null) {
            return emptyResponse(Response.BAD_REQUEST);
        }
        final var body = ByteBuffer.wrap(bytes);
        dao.upsert(key, body);
        return emptyResponse(Response.CREATED);
    }

    @NotNull
    private Response deleteEntity(@NotNull final ByteBuffer key) throws IOException {
        dao.remove(key);
        return emptyResponse(Response.ACCEPTED);
    }

    private void asyncExecute(
            @NotNull final HttpSession session,
            @NotNull final ResponseSupplier supplier) {
        asyncExecute(() -> {
            try {
                sendResponse(session, supplier.supply());
            } catch (Exception e) {
                log.error("Unable to create response", e);
            }
        });
    }

    @NotNull
    private static HttpServerConfig createConfig(
            final int port,
            final int workersCount) {
        final var acceptor = new AcceptorConfig();
        acceptor.port = port;

        final var config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.minWorkers = workersCount;
        config.maxWorkers = workersCount;
        return config;
    }

    @NotNull
    private static Response emptyResponse(@NotNull final String code) {
        return new Response(code, Response.EMPTY);
    }

    private static void sendResponse(
            @NotNull final HttpSession session,
            @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (Exception e) {
            try {
                log.error("Unable to send response", e);
                session.sendError(Response.INTERNAL_ERROR, null);
            } catch (Exception ex) {
                log.error("Unable to send error", e);
            }
        }
    }

    private static void sendEmptyResponse(
            @NotNull final HttpSession session,
            @NotNull final String code) {
        sendResponse(session, emptyResponse(code));
    }
}
