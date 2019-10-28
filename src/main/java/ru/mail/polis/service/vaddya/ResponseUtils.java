package ru.mail.polis.service.vaddya;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.nio.http.Request;
import one.nio.http.Response;

import static java.util.stream.Collectors.toList;

final class ResponseUtils {
    private static final Logger log = LoggerFactory.getLogger(ResponseUtils.class);
    private static final String HEADER_TIMESTAMP = "X-OK-Timestamp: ";
    static final String HEADER_PROXY = "X-OK-Proxy: True";

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

    @NotNull
    static Collection<Response> extract(@NotNull final Collection<Future<Response>> futures) {
        return futures.stream()
                .map(ResponseUtils::extractFuture)
                .filter(Objects::nonNull)
                .collect(toList());
    }

    @Nullable
    static Response extractFuture(@NotNull final Future<Response> future) {
        try {
            return future.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.debug("Unable to get response from remote node: {}", e.getMessage());
            return null;
        }
    }
}
