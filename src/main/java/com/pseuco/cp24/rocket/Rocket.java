package com.pseuco.cp24.rocket;

import com.pseuco.cp24.Config;
import com.pseuco.cp24.request.RequestHandler;

/**
 * <p>
 * Entrypoint of your implementation.
 * </p>
 *
 * <p>
 * ⚠️ This class must not be renamed and the signature of {@link launch} must
 * not be changed.
 * </p>
 */
public class Rocket {
    /**
     * Explicit default constructor.
     */
    public Rocket() {
    }

    /**
     * Starts the ticket sales system.
     *
     * @param config Configuration of the ticket sales system.
     * @return Request handler (load balancer) of the system.
     */
    public static RequestHandler launch(Config config) {
        if (config.getUseBonus()) {
            throw new RuntimeException("Bonus not implemented!");
        }
        final var coordinator = new Coordinator(config);
        // TODO: Launch coordinator and estimator.
        return coordinator.balancer;
    }
}
