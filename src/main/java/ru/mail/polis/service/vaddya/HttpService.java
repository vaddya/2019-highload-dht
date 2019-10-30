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
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.mail.polis.service.vaddya.ByteBufferUtils.wrapString;
import static ru.mail.polis.service.vaddya.ResponseUtils.emptyResponse;

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
                .collect(toMap(node -> node, this::createHttpClient));
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

        final var nodes = topology.primaryFor(id, rf);
        final var futures = schedule(nodes, node -> node.get(id));

        asyncExecute(() -> {
            log.debug("[{}] Gathering get responses: id={}", port, id.hashCode());
            final var values = ResponseUtils.extract(futures)
                    .stream()
                    .map(ResponseUtils::responseToValue)
                    .collect(toList());
            if (values.size() < rf.ack()) {
                session.sendEmptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
                log.debug("[{}] Not enough replicas for get request: id={}", port, id.hashCode());
                return;
            }
            final var value = Value.merge(values);
            session.send(value);
            log.debug("[{}] Get returned: id={}, value={}", port, id.hashCode(), value);
        });
    }

    @NotNull
    private Response getEntityLocal(@NotNull final String id) {
        log.debug("[{}] Get local entity: id={}", port, id.hashCode());
        final var key = wrapString(id);
        final var entry = dao.getEntry(key);
        final var value = Value.fromEntry(entry);
        return ResponseUtils.valueToResponse(value);
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
            asyncExecute(() -> session.send(putEntityLocal(id, bytes)));
            return;
        }

        final var nodes = topology.primaryFor(id, rf);
        final var futures = schedule(nodes, node -> node.put(id, bytes));

        asyncExecute(() -> {
            log.debug("[{}] Gathering put responses: id={}", port, id.hashCode());
            final var acks = ResponseUtils.extract(futures)
                    .stream()
                    .filter(ResponseUtils::is2xx)
                    .count();
            if (acks >= rf.ack()) {
                session.sendEmptyResponse(Response.CREATED);
                log.debug("[{}] Put created: id={}", port, id.hashCode());
                return;
            }
            session.sendEmptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
            log.debug("[{}] Not enough replicas for put request: id={}", port, id.hashCode());
        });
    }

    @NotNull
    private Response putEntityLocal(
            @NotNull final String id,
            @NotNull final byte[] bytes) {
        log.debug("[{}] Put local entity: id={}", port, id.hashCode());
        final var key = wrapString(id);
        final var value = ByteBuffer.wrap(bytes);
        dao.upsert(key, value);
        return emptyResponse(Response.CREATED);
    }

    private void scheduleDeleteEntity(
            @NotNull final ServiceSession session,
            @NotNull final String id,
            @NotNull final ReplicationFactor rf,
            final boolean proxied) {
        if (proxied) {
            asyncExecute(() -> session.send(deleteEntityLocal(id)));
            return;
        }

        final var nodes = topology.primaryFor(id, rf);
        final var futures = schedule(nodes, node -> node.delete(id));

        asyncExecute(() -> {
            log.debug("[{}] Gathering delete responses: id={}", port, id.hashCode());
            final var acks = ResponseUtils.extract(futures)
                    .stream()
                    .filter(ResponseUtils::is2xx)
                    .count();
            if (acks < rf.ack()) {
                session.sendEmptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
                log.debug("[{}] Not enough replicas for delete request: id={}", port, id.hashCode());
                return;
            }
            session.sendEmptyResponse(Response.ACCEPTED);
            log.debug("[{}] Delete accepted: id={}", port, id.hashCode());
        });
    }

    @NotNull
    private Response deleteEntityLocal(@NotNull final String id) {
        log.debug("[{}] Delete local entity: id={}", port, id.hashCode());
        final var key = wrapString(id);
        dao.remove(key);
        return emptyResponse(Response.ACCEPTED);
    }

    @NotNull
    private Collection<Future<Response>> schedule(
            @NotNull final Collection<String> nodes,
            @NotNull final Function<ServiceClient, Future<Response>> function) {
        return nodes.stream()
                .map(node -> function.apply(clients.get(node)))
                .collect(toList());
    }

    @NotNull
    private ServiceClient createHttpClient(@NotNull final String node) {
        if (topology.isMe(node)) {
            return new LocalServiceClient(workers);
        }
        return new HttpServiceClient(node, workers);
    }

    private final class LocalServiceClient implements ServiceClient {
        private final ExecutorService executor;

        private LocalServiceClient(@NotNull final ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        @NotNull
        public Future<Response> get(@NotNull final String id) {
            return executor.submit(() -> getEntityLocal(id));
        }

        @Override
        @NotNull
        public Future<Response> put(
                @NotNull final String id,
                @NotNull final byte[] data) {
            return executor.submit(() -> putEntityLocal(id, data));
        }

        @Override
        @NotNull
        public Future<Response> delete(
                @NotNull final String id) {
            return executor.submit(() -> deleteEntityLocal(id));
        }
    }
}
