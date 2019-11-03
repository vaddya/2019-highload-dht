package ru.mail.polis.service.vaddya;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

final class ResponseUtils {
    static final String HEADER_TIMESTAMP = "X-OK-Timestamp";
    static final String PROXY_HEADER = "X-OK-Proxy";
    static final String PROXY_TRUE = "True";
    static final String TRANSFER_ENCODING_CHUNKED = "Transfer-Encoding: chunked";

    private ResponseUtils() {
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }

    @NotNull
    static Response valueToResponse(@NotNull final Value value) {
        if (value.state() == Value.State.PRESENT) {
            final var response = Response.ok(value.data());
            response.addHeader(timestamp(value));
            return response;
        } else if (value.state() == Value.State.REMOVED) {
            final var response = emptyResponse(Response.NOT_FOUND);
            response.addHeader(timestamp(value));
            return response;
        }
        return emptyResponse(Response.NOT_FOUND);
    }

    @NotNull
    static Response valuesToResponse(@NotNull final Collection<Value> values) {
        final var value = Value.merge(values);
        return valueToResponse(value);
    }

    @NotNull
    static Response emptyResponse(@NotNull final String code) {
        return new Response(code, Response.EMPTY);
    }

    @NotNull
    private static String timestamp(@NotNull final Value value) {
        return HEADER_TIMESTAMP + ": " + value.ts();
    }
}
