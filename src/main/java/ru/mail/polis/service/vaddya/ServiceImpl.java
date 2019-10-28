package ru.mail.polis.service.vaddya;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.vaddya.DAOImpl;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vaddya.topology.ReplicationFactor;
import ru.mail.polis.service.vaddya.topology.Topology;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static ru.mail.polis.service.vaddya.ByteBufferUtils.wrapString;
import static ru.mail.polis.service.vaddya.ResponseUtils.emptyResponse;

public final class ServiceImpl extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(ServiceImpl.class);
    private static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

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
        this.quorum = ReplicationFactor.quorum(topology.all().size());
        this.dao = (DAOImpl) dao;
        this.clients = topology.others()
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
        ((ServiceSession) httpSession).sendEmptyResponse(Response.BAD_REQUEST);
    }

    @Override
    public synchronized void start() {
        super.start();
        log.debug("Server started on port {}", port);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        log.debug("Server stopped on port {}", port);
    }

    /**
     * Process heartbeat request and respond with empty OK response.
     *
     * @param httpSession HTTP session
     */
    @Path("/v0/status")
    public void status(@NotNull final HttpSession httpSession) {
        ((ServiceSession) httpSession).sendEmptyResponse(Response.OK);
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
        final var session = (ServiceSession) httpSession;
        if (id == null || id.isEmpty()) {
            session.sendEmptyResponse(Response.BAD_REQUEST);
            return;
        }

        final ReplicationFactor rf;
        try {
            rf = replicas == null ? quorum : ReplicationFactor.parse(replicas);
        } catch (IllegalArgumentException e) {
            log.warn("Wrong replication factor: {}", replicas);
            session.sendEmptyResponse(Response.BAD_REQUEST);
            return;
        }

        final var proxied = ResponseUtils.isProxied(request);
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
                log.warn("Not supported HTTP-method: {}", request.getMethod());
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
    @SuppressWarnings("OperatorPrecedence")
    public void entities(
            @Param("start") final String start,
            @Param("end") final String end,
            @NotNull final Request request,
            @NotNull final HttpSession httpSession) {
        final var session = (ServiceSession) httpSession;
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
        final var key = wrapString(id);
        log.debug("Scheduling get entity: rf={}, id={}", proxied ? "local" : rf, key.hashCode());
        if (proxied) {
//            asyncExecute(() -> session.send(getEntityLocal(key.duplicate())));
            session.send(getEntityLocal(key.duplicate()));
            return;
        }

        final var futures = topology.primaryFor(key, rf)
                .stream()
                .map(node -> topology.isMe(node)
                        ? submit(() -> getEntityLocal(key.duplicate()))
                        : clients.get(node).get(id))
                .collect(toList());

//        asyncExecute(() -> {
            log.debug("Gathering get responses: port={}, id={}", port, key.hashCode());
            final var values = ResponseUtils.extract(futures)
                    .stream()
                    .map(ResponseUtils::responseToValue)
                    .collect(toList());
            if (values.size() < rf.ack()) {
                session.sendEmptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
                log.debug("Not enough replicas for get request: port={}, id={}", port, key.hashCode());
                return;
            }
            session.send(Value.mergeValues(values));
            log.debug("Get returned: port={}, id={}", port, key.hashCode());
//        });
    }

    @NotNull
    private Response getEntityLocal(@NotNull final ByteBuffer key) {
        log.debug("Get local entity: port={}, id={}", port, key.hashCode());
        try {
            final var entry = dao.getEntry(key);
            final var value = Value.fromEntry(entry);
            return ResponseUtils.valueToResponse(value);
        } catch (NoSuchElementException e) {
            return emptyResponse(Response.NOT_FOUND);
        }
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

        final var key = wrapString(id);
        log.debug("Scheduling put entity: rf={}, id={}", proxied ? "local" : rf, key.hashCode());
        if (proxied) {
//            asyncExecute(() -> session.send(putEntityLocal(key.duplicate(), bytes)));
            session.send(putEntityLocal(key.duplicate(), bytes));
            return;
        }

        final var futures = topology.primaryFor(key, rf)
                .stream()
                .map(node -> topology.isMe(node)
                        ? submit(() -> putEntityLocal(key.duplicate(), bytes))
                        : clients.get(node).put(id, bytes))
                .collect(toList());

//        asyncExecute(() -> {
            log.debug("Gathering put responses: port={}, id={}", port, key.hashCode());
            final var responses = ResponseUtils.extract(futures);
            if (responses.size() < rf.ack()) {
                session.sendEmptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
                log.debug("Not enough replicas for put request: port={}, id={}", port, key.hashCode());
                return;
            }
            session.sendEmptyResponse(Response.CREATED);
            log.debug("Put created: port={}, id={}", port, key.hashCode());
//        });
    }

    @NotNull
    private Response putEntityLocal(
            @NotNull final ByteBuffer key,
            @NotNull final byte[] bytes) {
        log.debug("Put local entity: port={}, id={}", port, key.hashCode());
        final var body = ByteBuffer.wrap(bytes);
        dao.upsert(key, body);
        return emptyResponse(Response.CREATED);
    }

    private void scheduleDeleteEntity(
            @NotNull final ServiceSession session,
            @NotNull final String id,
            @NotNull final ReplicationFactor rf,
            final boolean proxied) {
        final var key = wrapString(id);
        log.debug("Scheduling delete entity: rf={}, id={}", proxied ? "local" : rf, key.hashCode());
        if (proxied) {
//            asyncExecute(() -> session.send(deleteEntityLocal(key.duplicate())));
            session.send(deleteEntityLocal(key.duplicate()));
            return;
        }

        final var futures = topology.primaryFor(key, rf)
                .stream()
                .map(node -> topology.isMe(node)
                        ? submit(() -> deleteEntityLocal(key.duplicate()))
                        : clients.get(node).delete(id))
                .collect(toList());

//        asyncExecute(() -> {
            log.debug("Gathering delete responses: port={}, id={}", port, key.hashCode());
            final var responses = ResponseUtils.extract(futures);
            if (responses.size() < rf.ack()) {
                session.sendEmptyResponse(RESPONSE_NOT_ENOUGH_REPLICAS);
                log.debug("Not enough replicas for delete request: port={}, id={}", port, key.hashCode());
                return;
            }
            session.sendEmptyResponse(Response.ACCEPTED);
            log.debug("Delete accepted: port={}, id={}", port, key.hashCode());
//        });
    }

    @NotNull
    private Response deleteEntityLocal(@NotNull final ByteBuffer key) {
        log.debug("Delete local entity: port={}, id={}", port, key.hashCode());
        dao.remove(key);
        return emptyResponse(Response.ACCEPTED);
    }

    @NotNull
    private Future<Response> submit(@NotNull final Supplier<Response> supplier) {
        log.debug("Local task submitted: port={}", port);
        return ((ExecutorService) workers).submit(supplier::get);
    }

    @NotNull
    private ServiceClient createHttpClient(@NotNull final String node) {
        return new ServiceClient(new ConnectionString(node + "?timeout=1000"), workers);
    }
}
