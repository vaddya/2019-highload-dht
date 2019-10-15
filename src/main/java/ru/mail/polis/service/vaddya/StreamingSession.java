package ru.mail.polis.service.vaddya;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

final class StreamingSession extends HttpSession {
    private static final byte LF = '\n';
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] EMPTY_CHUNK = "0\r\n\r\n".getBytes(Charsets.UTF_8);

    private Iterator<Record> records;

    StreamingSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    void streamRange(@NotNull final Iterator<Record> records) throws IOException {
        this.records = records;

        final var response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);

        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();

        next();
    }

    private void next() throws IOException {
        while (records.hasNext() && queueHead == null) {
            final var chunk = recordToChunk(records.next());
            write(chunk, 0, chunk.length);
        }

        if (!records.hasNext()) {
            write(EMPTY_CHUNK, 0, EMPTY_CHUNK.length);
            server.incRequestsProcessed();
            tryExtractNextRequest();
        }
    }

    private void tryExtractNextRequest() {
        if ((handling = pipeline.pollFirst()) != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                try {
                    server.handleRequest(handling, this);
                } catch (IOException e) {
                    log.error("Unable to process next request", e);
                }
            }
        }
    }

    private static byte[] recordToChunk(@NotNull final Record record) {
        final var key = record.getKey();
        final var value = record.getValue();

        final var keyLength = key.remaining();
        final var valueLength = value.remaining();
        final var payloadLength = keyLength + 1 + valueLength;
        final var chunkHexSize = Integer.toHexString(payloadLength);
        final var chunkLength = chunkHexSize.length() + 2 + payloadLength + 2;

        final var chunk = new byte[chunkLength];
        ByteBuffer.wrap(chunk)
                .put(chunkHexSize.getBytes(Charsets.UTF_8))
                .put(CRLF)
                .put(key)
                .put(LF)
                .put(value)
                .put(CRLF);

        return chunk;
    }
}