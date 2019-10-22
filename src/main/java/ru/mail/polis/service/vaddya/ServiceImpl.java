package ru.mail.polis.service.vaddya;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vaddya.topology.Topology;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static ru.mail.polis.service.vaddya.ByteBufferUtils.unwrapBytes;
import static ru.mail.polis.service.vaddya.ByteBufferUtils.wrapString;

public class ServiceImpl extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(ServiceImpl.class);

    private final DAO dao;
    private final Topology<String> topology;
    private final Map<String, HttpClient> httpClients;

    /**
     * Create a {@link HttpServer} instance that implements {@link Service}.
     *
     * @param port         a server port
     * @param topology     a cluster topology
     * @param dao          a data storage
     * @param workersCount a number of worker threads
     * @return a server instance
     * @throws IOException if an IO error occurred
     */
    @NotNull
    public static Service create(
            final int port,
            @NotNull final Topology<String> topology,
            @NotNull final DAO dao,
            final int workersCount) throws IOException {
        final var acceptor = new AcceptorConfig();
        acceptor.port = port;

        final var config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.minWorkers = workersCount;
        config.maxWorkers = workersCount;

        return new ServiceImpl(config, topology, dao);
    }

    private ServiceImpl(
            @NotNull final HttpServerConfig config,
            @NotNull final Topology<String> topology,
            @NotNull final DAO dao) throws IOException {
        super(config);

        this.topology = topology;
        this.dao = dao;
        this.httpClients = topology.others()
                .stream()
                .collect(toMap(identity(), ServiceImpl::createHttpClient));
    }

    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession session) {
        sendEmptyResponse(session, Response.BAD_REQUEST);
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) {
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
        final var node = topology.primaryFor(key);
        if (!topology.isMe(node)) {
            asyncExecute(session, () -> proxy(node, request));
            return;
        }

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
                break;
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
        if (start == null || start.isEmpty()) {
            sendEmptyResponse(session, Response.BAD_REQUEST);
            return;
        }
        if (end != null && end.isEmpty()) {
            sendEmptyResponse(session, Response.BAD_REQUEST);
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            sendEmptyResponse(session, Response.METHOD_NOT_ALLOWED);
            return;
        }
        final var startBuffer = wrapString(start);
        final var endBuffer = end == null ? null : wrapString(end);
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

    @NotNull
    private Response proxy(
            @NotNull final String node,
            @NotNull final Request request) throws IOException {
        try {
            return httpClients.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            log.error("Unable to proxy request", e);
            throw new IOException("Unable to proxy request", e);
        }
    }

    private void asyncExecute(
            @NotNull final HttpSession session,
            @NotNull final ResponseSupplier supplier) {
        asyncExecute(() -> {
            try {
                sendResponse(session, supplier.supply());
            } catch (IOException e) {
                log.error("Unable to create response", e);
                sendEmptyResponse(session, Response.INTERNAL_ERROR);
            }
        });
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
        } catch (IOException e) {
            try {
                log.error("Unable to send response", e);
                session.sendError(Response.INTERNAL_ERROR, null);
            } catch (IOException ex) {
                log.error("Unable to send error", e);
            }
        }
    }

    private static void sendEmptyResponse(
            @NotNull final HttpSession session,
            @NotNull final String code) {
        sendResponse(session, emptyResponse(code));
    }

    @NotNull
    private static HttpClient createHttpClient(@NotNull final String node) {
        return new HttpClient(new ConnectionString(node + "?timeout=100"));
    }

    @FunctionalInterface
    private interface ResponseSupplier {
        @NotNull
        Response supply() throws IOException;
    }
}
