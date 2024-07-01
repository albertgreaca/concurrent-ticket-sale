package com.pseuco.cp24.request;

/**
 * <p>
 * Interface for handling requests from a web browser.
 * </p>
 *
 * <p>
 * The load balancer must implement this interface.
 * </p>
 */
public interface RequestHandler {
    /**
     * <p>
     * Handle a request from a web browser.
     * </p>
     *
     * <p>
     * ⚠️ This method may be called from different threads!
     * </p>
     *
     * @param request {@link Request} to be handled.
     */
    public void handle(Request request);

    /**
     * <p>
     * Shut the ticket sales system down.
     * </p>
     *
     * <p>
     * When this method returns, all threads spawned for the ticket sales system
     * (e.g., the servers and the estimator) must have terminated.
     * </p>
     */
    public void shutdown();
}
