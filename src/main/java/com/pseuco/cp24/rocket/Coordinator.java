package com.pseuco.cp24.rocket;

import java.util.List;

import com.pseuco.cp24.Config;

/**
 * <p>
 * {@link Coordinator} that orchestrates all the components of the system.
 * </p>
 */
public class Coordinator {
    /**
     * Configuration of the system.
     */
    private final Config config;

    /**
     * Database of the system.
     */
    protected final Database database;
    /**
     * Load balancer of the system.
     */
    protected final Balancer balancer;
    /**
     * Estimator of the system.
     */
    protected final Estimator estimator;

    /**
     * Constructs a new {@link Coordinator}.
     *
     * @param config Configuration of the system.
     */
    public Coordinator(final Config config) {
        this.config = config;
        this.database = new Database(this);
        this.balancer = new Balancer(this);
        this.estimator = new Estimator(this, config.getEstimatorRoundtripTime());
    }

    /**
     * Returns the configuration of the system.
     *
     * @return Configuration of the system.
     */
    public Config getConfig() {
        return this.config;
    }

    /**
     * Returns the database of the system.
     *
     * @return Database of the system.
     */
    public Database getDatabase() {
        return this.database;
    }

    /**
     * Returns the mailbox of the estimator of the system.
     *
     * @return Mailbox of the estimator of the system.
     */
    public Mailbox<Command<Estimator>> getEstimatorMailbox() {
        return this.estimator.getMailbox();
    }

    /**
     * <p>
     * Picks a random server among the active (non-terminating) servers.
     * </p>
     *
     * <p>
     * 📌 Hint: Useful for assigning (new) servers to clients.
     * </p>
     *
     * @return Randomly picked {@link Server}.
     */
    public Server pickRandomServer() {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * <p>
     * Removes a server.
     * </p>
     *
     * <p>
     * 📌 Hint: Should be called after a server has terminated to completely remove
     * it from the system.
     * </p>
     *
     * @param server {@link Server} to remove.
     */
    public void removeServer(final Server server) {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * <p>
     * Spins up a new server.
     * </p>
     *
     * <p>
     * 📌 Hint: Use this to start new servers for on-demand scaling.
     * </p>
     *
     * @return The new {@link Server}.
     */
    public Server createServer() {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * Scales the system to the given number of servers.
     *
     * @param numServers Number of servers.
     * @return Number of servers.
     */
    public int scale(int numServers) {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * Returns the number of active (non-terminating) servers.
     *
     * @return Number of active (non-terminating) servers.
     */
    public int getNumOfServers() {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * Returns a list of the active {@link Server}s.
     *
     * @return A list of the active {@link Server}s.
     */
    public List<Server> getActiveServerIds() {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * <p>
     * Returns a list of all {@link Server}s.
     * </p>
     *
     * <p>
     * 📌 Hint: Use this in the {@link Estimator} to send messages to all servers
     * (and not just those which are still active).
     * </p>
     *
     * @return A list of all {@link Server}s.
     */
    public List<Server> getAllServerIds() {
        throw new RuntimeException("Not implemented!");
    }

}
