package ru.mail.polis.service.vaddya;

import org.jetbrains.annotations.NotNull;

import one.nio.http.Request;
import one.nio.http.Response;

final class ResponseUtils {
    static final String HEADER_PROXY = "X-OK-Proxy: True";
    private static final String HEADER_TIMESTAMP = "X-OK-Timestamp: ";

    private ResponseUtils() {
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(HEADER_PROXY) != null;
    }

    @NotNull
    static Response valueToResponse(@NotNull final Value value) {
        if (value.state() == Value.State.PRESENT) {
            final var response = Response.ok(value.data());
            response.addHeader(ResponseUtils.HEADER_TIMESTAMP + value.ts());
            return response;
        } else if (value.state() == Value.State.REMOVED) {
            final var response = emptyResponse(Response.NOT_FOUND);
            response.addHeader(ResponseUtils.HEADER_TIMESTAMP + value.ts());
            return response;
        }
        return emptyResponse(Response.NOT_FOUND);
    }

    @NotNull
    static Value responseToValue(@NotNull final Response response) {
        final var ts = response.getHeader(HEADER_TIMESTAMP);
        if (response.getStatus() == 200) {
            if (ts == null) {
                throw new IllegalArgumentException();
            }
            return Value.present(response.getBody(), Long.parseLong(ts));
        } else {
            if (ts == null) {
                return Value.absent();
            }
            return Value.removed(Long.parseLong(ts));
        }
    }

    @NotNull
    static Response emptyResponse(@NotNull final String code) {
        return new Response(code, Response.EMPTY);
    }
}
