package com.pseuco.cp24.request;

import java.io.IOException;
import java.util.Optional;

import com.sun.net.httpserver.HttpExchange;

/**
 * <p>
 * Represents a request from a web browser.
 * </p>
 *
 * <p>
 * 📌 Hint: Your implementation primarily interacts with instances of this
 * class.
 * </p>
 *
 * <p>
 * Every request has a {@link Method} and a {@link Kind}. The {@link Method}
 * indicates whether a request is merely for retrieving information or may have
 * side effects like scaling the number of servers or reserving a ticket.
 * </p>
 */
public class HttpRequest extends Request {

    /**
     * Underlying {@link HttpExchange} used for communication.
     */
    private final HttpExchange exchange;

    /**
     * Constructs a new request from the provided parameters.
     *
     * @param method   Method of the request.
     * @param kind     Kind of the request.
     * @param exchange {@link HttpExchange} used for communication.
     */
    public HttpRequest(final Method method, final Kind kind, final HttpExchange exchange) {
        super(method, kind, CustomerId.fromHttpExchange(exchange), ServerId.fromHttpExchange(exchange));
        this.exchange = exchange;
    }

    @Override
    public String getPath() {
        return this.exchange.getRequestURI().getPath();
    }

    @Override
    public void setServerId(final ServerId serverId) {
        this.serverId = Optional.of(serverId);
        this.exchange.getResponseHeaders().set(ServerId.HEADER_NAME, serverId.getUUID().toString());
    }

    @Override
    public Optional<Integer> readInt() {
        try {
            final var body = new String(this.exchange.getRequestBody().readAllBytes());
            return Optional.of(Integer.valueOf(body));
        } catch (IOException | NumberFormatException error) {
            return Optional.empty();
        }
    }

    /**
     * <p>
     * Sends a response to the client.
     * </p>
     *
     * <p>
     * This method blocks until the response has been sent.
     * </p>
     *
     * <p>
     * 📌 Hint: You do not need to call this method directly. Instead, use any
     * of the other methods for sending a response with specific body.
     * </p>
     *
     * @param code HTTP status code of the response.
     * @param body Body to send to the client.
     */
    protected void respond(final int code, final String body) {
        final var bytes = body.getBytes();
        try {
            this.exchange.sendResponseHeaders(code, bytes.length);
            try (java.io.OutputStream stream = exchange.getResponseBody()) {
                stream.write(bytes);
            }
        } catch (IOException error) {
            System.err.println("Warning: Error responding to client.");
        }
    }

    @Override
    public void respondWithError(final String message) {
        this.respond(400, message == null ? "" : message);
    }

    @Override
    public void respondWithInt(final int integer) {
        this.respond(200, Integer.toString(integer));
    }

    @Override
    public void respondWithString(final String string) {
        this.respond(200, string);
    }

    @Override
    public void respondWithSoldOut() {
        this.respond(200, "SOLD OUT");
    }

    @Override
    public void respondWithServerIds(final Iterable<ServerId> ids) {
        final var serverList = new StringBuilder();
        for (var serverId : ids) {
            serverList.append(serverId.getUUID().toString());
            serverList.append('\n');
        }
        this.respond(200, serverList.toString());
    }
}
