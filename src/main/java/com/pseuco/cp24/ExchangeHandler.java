package com.pseuco.cp24;

import java.io.IOException;

import com.sun.net.httpserver.HttpHandler;
import com.pseuco.cp24.request.Request;
import com.pseuco.cp24.request.HttpRequest;
import com.pseuco.cp24.request.RequestHandler;
import com.sun.net.httpserver.HttpExchange;

/**
 * <p>
 * Handler for wrapping {@link HttpExchange} and handing them over to a request handler.
 * </p>
 *
 * <p>
 * 📌 Hint: You do never have to interact with this class and hence, can ignore it.
 * </p>
 */
class ExchangeHandler implements HttpHandler {
    /**
     * Request handler to hand requests over to.
     */
    private final RequestHandler requestHandler;

    /**
     * Constructs a new HTTP handler with the given request handler.
     *
     * @param requestHandler Request handler to hand requests over to.
     */
    public ExchangeHandler(final RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    /**
     * <p>
     * Handles an HTTP request and hands it over to the request handler.
     * </p>
     *
     * <p>
     * 📌 Hint: You do not need to understand the involved HTTP wizardry.
     * </p>
     */
    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        // Set the CORS access control headers.
        final var headers = exchange.getResponseHeaders();
        headers.set("Access-Control-Request-Method", "*");
        headers.set("Access-Control-Allow-Origin", "*");
        headers.set("Access-Control-Allow-Headers", "*");
        headers.set("Access-Control-Expose-Headers", "*");
        // CORS pre-flight requests (OPTIONS) are handled directly with a 204 (No Content).
        if (exchange.getRequestMethod().equals("OPTIONS")) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }
        final var method = Request.Method.fromName(exchange.getRequestMethod());
        if (method.isEmpty()) {
            // Tell the client which methods are allowed (405).
            exchange.getResponseHeaders().set("Allow", "GET, POST");
            exchange.sendResponseHeaders(405, 0);
            exchange.close();
            return;
        }
        final var kind = Request.Kind.fromPath(exchange.getRequestURI().getPath());
        if (kind.isEmpty()) {
            // Tell the client that the requested path does not exist (404).
            var notFound = "404: Not Found!".getBytes();
            exchange.sendResponseHeaders(404, notFound.length);
            exchange.getResponseBody().write(notFound);
            exchange.close();
            return;
        }
        // Hand over the request to the request handler.
        final var request = new HttpRequest(method.get(), kind.get(), exchange);
        try {
            this.requestHandler.handle(request);
        } catch (RuntimeException error) {
            // We catch those errors because otherwise they are silently swallowed.
            System.err.println("Runtime error while handling request!");
            error.printStackTrace();
            exchange.close();
        }
    }
}
