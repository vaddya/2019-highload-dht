package ru.mail.polis.service.vaddya;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static ru.mail.polis.service.vaddya.ResponseUtils.HEADER_TIMESTAMP;
import static ru.mail.polis.service.vaddya.ResponseUtils.PROXY_HEADER;
import static ru.mail.polis.service.vaddya.ResponseUtils.PROXY_TRUE;

final class HttpServiceClient implements ServiceClient {
    private static final Logger log = LoggerFactory.getLogger(HttpServiceClient.class);
    private static final String PATH_ENTITY = "/v0/entity";
    private static final int TIMEOUT_MILLIS = 200;

    private final String baseUrl;
    private final HttpClient client;

    HttpServiceClient(@NotNull final String baseUrl) {
        this.baseUrl = baseUrl;
        this.client = HttpClient.newBuilder()
                .build();
    }

    @Override
    @NotNull
    public CompletableFuture<Value> getAsync(@NotNull final String id) {
        final var request = request(id).GET().build();
        log.debug("Schedule get remote entity: uri={}, id={}", baseUrl, id.hashCode());
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(HttpServiceClient::toValue);
    }

    @Override
    @NotNull
    public CompletableFuture<Void> putAsync(
            @NotNull final String id,
            @NotNull final byte[] data) {
        final var request = request(id).PUT(bytes(data)).build();
        log.debug("Schedule put remote entity: uri={}, id={}", baseUrl, id.hashCode());
        return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(x -> null);
    }

    @Override
    @NotNull
    public CompletableFuture<Void> deleteAsync(@NotNull final String id) {
        final var request = request(id).DELETE().build();
        log.debug("Schedule delete remote entity: uri={}, id={}", baseUrl, id.hashCode());
        return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(x -> null);
    }

    @NotNull
    private HttpRequest.Builder request(@NotNull final String id) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + PATH_ENTITY + "?id=" + id))
                .header(PROXY_HEADER, PROXY_TRUE)
                .timeout(Duration.ofMillis(TIMEOUT_MILLIS));
    }

    @NotNull
    private static HttpRequest.BodyPublisher bytes(@NotNull final byte[] data) {
        return HttpRequest.BodyPublishers.ofByteArray(data);
    }

    @NotNull
    private static Value toValue(@NotNull final HttpResponse<byte[]> response) {
        final var ts = response.headers().firstValueAsLong(HEADER_TIMESTAMP);
        if (response.statusCode() == 200) {
            if (ts.isEmpty()) {
                throw new IllegalArgumentException();
            }
            return Value.present(response.body(), ts.getAsLong());
        } else {
            if (ts.isEmpty()) {
                return Value.absent();
            }
            return Value.removed(ts.getAsLong());
        }
    }
}
