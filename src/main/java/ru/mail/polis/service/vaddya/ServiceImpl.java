package ru.mail.polis.service.vaddya;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;

public class ServiceImpl extends HttpServer implements Service {
    private static final Log LOG = LogFactory.getLog(ServiceImpl.class);
    private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;
    
    private final DAO dao;
    
    public ServiceImpl(
            final int port,
            @NotNull final DAO dao) throws IOException {
        super(createConfig(port));
        this.dao = dao;
    }
    
    @Override
    public void handleDefault(
            @NotNull final Request request,
            @NotNull final HttpSession session) throws IOException {
        session.sendResponse(emptyResponse(Response.BAD_REQUEST));
    }
    
    @Path("/v0/status")
    public Response status() {
        return emptyResponse(Response.OK);
    }
    
    @Path("/v0/entity")
    public Response entity(
            @Param("id") final String id,
            final Request request) {
        if (id == null || id.isEmpty()) {
            return emptyResponse(Response.BAD_REQUEST);
        }
        final var key = wrapString(id);
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return getEntity(key);
                case Request.METHOD_PUT:
                    return putEntity(key, request.getBody());
                case Request.METHOD_DELETE:
                    return deleteEntity(key);
                default:
                    LOG.warn("Not supported HTTP-method: " + request.getMethod());
                    return emptyResponse(Response.METHOD_NOT_ALLOWED);
            }
        } catch (IOException e) {
            LOG.error("IO exception during request handling for id=" + id, e);
            return emptyResponse(Response.INTERNAL_ERROR);
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
    private static HttpServerConfig createConfig(final int port) {
        final var acceptor = new AcceptorConfig();
        acceptor.port = port;
        
        final var config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }
    
    @NotNull
    private static Response emptyResponse(String code) {
        return new Response(code, Response.EMPTY);
    }
    
    @NotNull
    private static ByteBuffer wrapString(@NotNull final String str) {
        return wrapString(str, DEFAULT_CHARSET);
    }
    
    @NotNull
    private static ByteBuffer wrapString(
            @NotNull final String str,
            @NotNull final Charset charset) {
        return ByteBuffer.wrap(str.getBytes(charset));
    }
    
    @NotNull
    private static byte[] unwrapBytes(@NotNull final ByteBuffer buffer) {
        final var duplicate = buffer.duplicate();
        final var bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
