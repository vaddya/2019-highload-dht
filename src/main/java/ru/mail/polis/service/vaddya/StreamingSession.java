package ru.mail.polis.service.vaddya;

import java.io.IOException;
import java.util.Iterator;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;

final class StreamingSession extends HttpSession {
    private Iterator<Record> records;

    StreamingSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    void sendRange(@NotNull final Iterator<Record> records) throws IOException {
        this.records = records;

        sendResponse(Response.ok(Response.EMPTY));
    }
}
