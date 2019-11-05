package ru.mail.polis.service.vaddya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.vaddya.DAOImpl;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vaddya.topology.ReplicationFactor;
import ru.mail.polis.service.vaddya.topology.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.mail.polis.service.vaddya.ByteBufferUtils.wrapString;
import static ru.mail.polis.service.vaddya.ResponseUtils.emptyResponse;

@SuppressWarnings("FutureReturnValueIgnored")
public final class HttpService extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(HttpService.class);
    private static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private static final int MIN_WORKERS = 4;

    private final DAOImpl dao;
    private final Topology<String> topology;
    private final ReplicationFactor quorum;
    private final Map<String, ServiceClient> clients;

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
        final var workers = Math.max(workersCount, MIN_WORKERS);
        log.info("[{}] Workers count: {}", port, workers);
        config.minWorkers = workers;
        config.maxWorkers = workers;

        return new HttpService(config, topology, dao);
    }

    private HttpService(
            @NotNull final HttpServerConfig config,
            @NotNull final Topology<String> topology,
            @NotNull final DAO dao) throws IOException {
        super(config);

        this.topology = topology;
        this.quorum = ReplicationFactor.quorum(topology.size());
        this.dao = (DAOImpl) dao;
        this.clients = topology.all()
                .stream()
                .collect(toMap(node -> node, this::createServiceClient));
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) {
        return new ServiceSession(socket, this);
    }

    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession httpSession) {
        ServiceSession.cast(httpSession).sendEmptyResponse(Response.BAD_REQUEST);
    }

    @Override
    public synchronized void start() {
        super.start();
        log.info("[{}] Server started", port);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        log.info("[{}] Server stopped", port);
    }

    /**
     * Process heartbeat request and respond with empty OK response.
     *
     * @param httpSession HTTP session
     */
    @Path("/v0/status")
    public void status(@NotNull final HttpSession httpSession) {
        ServiceSession.cast(httpSession).sendEmptyResponse(Response.OK);
    }

    /**
     * Process request to get, put or delete an entity by ID
     * and write response to the session instance.
     *
     * @param id          entity ID
     * @param replicas    replication factor in format "ack/from"
     * @param request     HTTP request
     * @param httpSession HTTP session
     */
    @Path("/v0/entity")
    public void entity(
            @Param("id") final String id,
            @Param("replicas") final String replicas,
            @NotNull final Request request,
            @NotNull final HttpSession httpSession) {
        final var session = ServiceSession.cast(httpSession);
        if (id == null || id.isEmpty()) {
            session.sendEmptyResponse(Response.BAD_REQUEST);
            return;
        }

        final var proxied = ResponseUtils.isProxied(request);
        final ReplicationFactor rf;
        try {
            rf = proxied || replicas == null ? quorum : ReplicationFactor.parse(replicas);
        } catch (IllegalArgumentException e) {
            log.debug("[{}] Wrong replication factor: {}", port, replicas);
            session.sendEmptyResponse(Response.BAD_REQUEST);
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                scheduleGetEntity(session, id, rf, proxied);
                break;
            case Request.METHOD_PUT:
                schedulePutEntity(session, id, request.getBody(), rf, proxied);
                break;
            case Request.METHOD_DELETE:
                scheduleDeleteEntity(session, id, rf, proxied);
                break;
            default:
                log.debug("[{}] Not supported HTTP-method: {}", port, request.getMethod());
                session.sendEmptyResponse(Response.METHOD_NOT_ALLOWED);
                break;
        }
    }

    /**
     * Process request to get range of values.
     *
     * @param start       Start key
     * @param end         End key
     * @param request     HTTP request
     * @param httpSession HTTP session
     */
    @Path("/v0/entities")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final Request request,
            @NotNull final HttpSession httpSession) {
        final var session = ServiceSession.cast(httpSession);
        if (start == null || start.isEmpty()) {
            session.sendEmptyResponse(Response.BAD_REQUEST);
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            session.sendEmptyResponse(Response.METHOD_NOT_ALLOWED);
            return;
        }

        final var startBuffer = wrapString(start);
        final var endBuffer = end == null ? null : wrapString(end);
        try {
            final var range = dao.range(startBuffer, endBuffer);
            session.stream(range);
        } catch (IOException e) {
            log.error("Unable to stream range of values", e);
        }
    }

    private void scheduleGetEntity(
            @NotNull final ServiceSession session,
            @NotNull final String id,
            @NotNull final ReplicationFactor rf,
            final boolean proxied) {
        if (proxied) {
            asyncExecute(() -> session.send(getEntityLocal(id)));
            return;
        }

        final var futures = topology.primaryFor(id, rf)
                .stream()
                .map(clients::get)
                .map(client -> client.getAsync(id))
                .collect(toList());

        CompletableFutureUtils.firstN(futures, rf.ack())
                .handle((res, e) -> handleResponses(res, e, ResponseUtils::valuesToResponse))
                .thenAccept(session::send);
    }

    @NotNull
    private Value getEntityLocal(@NotNull final String id) {
        log.debug("[{}] Get local entity: id={}", port, id.hashCode());
        final var key = wrapString(id);
        final var entry = dao.getEntry(key);
        return Value.fromEntry(entry);
    }

    private void schedulePutEntity(
            @NotNull final ServiceSession session,
            @NotNull final String id,
            @Nullable final byte[] bytes,
            @NotNull final ReplicationFactor rf,
            final boolean proxied) {
        if (bytes == null) {
            session.sendEmptyResponse(Response.BAD_REQUEST);
            return;
        }

        if (proxied) {
            asyncExecute(() -> {
                putEntityLocal(id, bytes);
                session.sendEmptyResponse(Response.CREATED);
            });
            return;
        }

        final var futures = topology.primaryFor(id, rf)
                .stream()
                .map(clients::get)
                .map(client -> client.putAsync(id, bytes))
                .collect(toList());

        CompletableFutureUtils.firstN(futures, rf.ack())
                .handle((res, e) -> handleResponses(res, e, voids -> emptyResponse(Response.CREATED)))
                .thenAccept(session::send);
    }

    private void putEntityLocal(
            @NotNull final String id,
            @NotNull final byte[] bytes) {
        log.debug("[{}] Put local entity: id={}", port, id.hashCode());
        final var key = wrapString(id);
        final var value = ByteBuffer.wrap(bytes);
        dao.upsert(key, value);
    }

    private void scheduleDeleteEntity(
            @NotNull final ServiceSession session,
            @NotNull final String id,
            @NotNull final ReplicationFactor rf,
            final boolean proxied) {
        if (proxied) {
            asyncExecute(() -> {
                deleteEntityLocal(id);
                session.sendEmptyResponse(Response.ACCEPTED);
            });
            return;
        }

        final var futures = topology.primaryFor(id, rf)
                .stream()
                .map(clients::get)
                .map(client -> client.deleteAsync(id))
                .collect(toList());

        CompletableFutureUtils.firstN(futures, rf.ack())
                .handle((res, e) -> handleResponses(res, e, voids -> emptyResponse(Response.ACCEPTED)))
                .thenAccept(session::send);
    }

    private void deleteEntityLocal(@NotNull final String id) {
        log.debug("[{}] Delete local entity: id={}", port, id.hashCode());
        final var key = wrapString(id);
        dao.remove(key);
    }

    @NotNull
    private <T> Response handleResponses(
            @Nullable final Collection<T> responses,
            @Nullable final Throwable error,
            @NotNull final Function<Collection<T>, Response> response) {
        if (responses != null && error == null) {
            return response.apply(responses);
        }
        if (error instanceof NotEnoughReplicasException) {
            log.debug("[{}] Not enough replicas to handle request: {}", port, error.getMessage());
            return emptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
        }
        log.error("[{}] Unknown response error", port, error);
        return emptyResponse(Response.INTERNAL_ERROR);
    }

    @NotNull
    private ServiceClient createServiceClient(@NotNull final String node) {
        if (topology.isMe(node)) {
            return new LocalServiceClient(workers);
        }
        return new HttpServiceClient(node);
    }

    private final class LocalServiceClient implements ServiceClient {
        private final ExecutorService executor;

        private LocalServiceClient(@NotNull final ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        @NotNull
        public CompletableFuture<Value> getAsync(@NotNull final String id) {
            return CompletableFuture.supplyAsync(() -> getEntityLocal(id), executor);
        }

        @Override
        @NotNull
        public CompletableFuture<Void> putAsync(
                @NotNull final String id,
                @NotNull final byte[] data) {
            return CompletableFuture.supplyAsync(() -> {
                putEntityLocal(id, data);
                return null;
            }, executor);
        }

        @Override
        @NotNull
        public CompletableFuture<Void> deleteAsync(@NotNull final String id) {
            return CompletableFuture.supplyAsync(() -> {
                deleteEntityLocal(id);
                return null;
            }, executor);
        }
    }
}
