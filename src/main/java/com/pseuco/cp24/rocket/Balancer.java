package com.pseuco.cp24.rocket;

import com.pseuco.cp24.request.Request;
import com.pseuco.cp24.request.RequestHandler;

/**
 * <p>
 * Implementation of the load balancer.
 * </p>
 *
 * <p>
 * Load balancer must implement {@link RequestHandler} for handling
 * requests. Note that the {@link handle} method may be called concurrently by
 * our framework from within different threads. Hence, your implementation
 * should use proper synchronization to avoid data races and other concurrency
 * related problems.
 * </p>
 *
 * <p>
 * The {@link Balancer} is constructed with a {@link Coordinator}, e.g., for
 * querying options, starting new servers, or retrieving the mailboxes of
 * servers.
 * </p>
 */
public class Balancer implements RequestHandler {
    /**
     * {@link Coordinator} of the ticket sales system.
     */
    private final Coordinator coordinator;

    /**
     * Constructs a new {@link Balancer}.
     *
     * @param coordinator {@link Coordinator} of the ticket sales system.
     */
    public Balancer(final Coordinator coordinator) {
        this.coordinator = coordinator;
        // Scale to the number of initial servers.
        this.coordinator.scale(this.coordinator.getConfig().getInitialServers());
    }

    // 📌 Hint: Look into the `RequestHandler` interface definition for specification
    // docstrings of `handle()` and `shutdown()`.

    @Override
    public void handle(final Request request) {
        /*
         * Implementation of the load balancer.
         *
         * Hint: You must handle the `NUM_SERVERS` and `GET_SERVERS` requests here. All
         * other requests must be redirected to individual servers (expect the `DEBUG`
         * request which can be handled however you like).
         *
         * For the `NUM_SERVERS` request you have to handle both `GET` and `POST` requests.
         */
        switch (request.getKind()) {
            case NUM_SERVERS -> {
                switch (request.getMethod()) {
                    case GET -> {
                        /*
                         * TODO: Query the coordinator for the number of active
                         * (non-terminating) servers and send the number back to
                         * the client using `respondWithInt`.
                         */
                        throw new RuntimeException("Not implemented!");
                    }
                    case POST -> {
                        /*
                         * TODO: Obtain the new number of servers from the request using
                         * `readInt`, scale to the given number of servers, and finally
                         * respond with the new number of servers.
                         */
                        throw new RuntimeException("Not implemented!");
                    }
                }
            }
            case GET_SERVERS -> {
                /**
                 * TODO: Query the coordinator for the active (non-terminating) servers
                 * and send the ids of these servers to the client.
                 *
                 * Hint: Use `respondWithServerIds`.
                 */
                throw new RuntimeException("Not implemented!");
            }

            case DEBUG -> {
                /**
                 * You are free to handle this request however you like, e.g., by sending
                 * some useful debugging information to the client.
                 */
                request.respondWithString("Happy Debugging! 🚫🐛");
            }

            default -> {
                /**
                 * TODO: The remaining requests must be handed over to a server.
                 *
                 * Hint: Your implementation must be able to handle cases where a server
                 * already terminated or is in the process of terminating when a request
                 * for that server hits the load balancer. In case the server has already
                 * terminated, the balancer should assign a new server to the client.
                 *
                 * You must use the coordinator to obtain the mailbox of the server which
                 * should handle the request. You must then use the mailbox of this server
                 * to deliver the request as a message with low priority. Using our
                 * skeleton this means constructing and sending a `MsgProcessRequest`
                 * message to the server.
                 */
                throw new RuntimeException("Not implemented!");
            }
        }
    }

    @Override
    public void shutdown() {
        throw new RuntimeException("Not implemented!");
    }
}
