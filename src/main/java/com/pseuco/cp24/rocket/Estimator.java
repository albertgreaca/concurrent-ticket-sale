package com.pseuco.cp24.rocket;

/**
 * <p>
 * {@link Estimator} that estimates the number of tickets available overall.
 * </p>
 */
public class Estimator implements Runnable {
    /**
     * {@link Coordinator} of the ticket sales system.
     */
    private final Coordinator coordinator;

    /**
     * Mailbox of the {@link Estimator}.
     */
    private final Mailbox<Command<Estimator>> mailbox = new Mailbox<>();

    /**
     * <p>
     * Constructs a new {@link Estimator}.
     * </p>
     * 
     * <p>
     * 📌 Hint: <code>roundtripSecs</code> is the time in seconds the estimator needs to
     * contact all servers. If there are <code>N</code> servers, then the estimator should
     * wait <code>roundtripSecs / N</code> between each server when collecting statistics.
     * </p>
     *
     * @param coordinator   {@link Coordinator} of the ticket sales system.
     * @param roundtripSecs Time the estimator takes to contact all servers.
     */
    public Estimator(Coordinator coordinator, int roundtripSecs) {
        this.coordinator = coordinator;
    }

    /**
     * Returns the {@link Mailbox} of the estimator.
     *
     * @return {@link Mailbox} of the estimator.
     */
    public Mailbox<Command<Estimator>> getMailbox() {
        return this.mailbox;
    }

    @Override
    public void run() {
        /*
         * TODO: Implement the estimator as described in the project description. The
         * estimator will periodically send messages to the servers and process the
         * messages from its own mailbox.
         */
        throw new RuntimeException("Not implemented!");
    }

    /**
     * A message informing the {@link Estimator} about tho number of available
     * tickets allocated to a particular server.
     */
    public static class MsgAvailableServer implements Command<Estimator> {
        private final Server server;
        private final int numAvailable;

        /**
         * Constructs a new {@link MsgAvailableServer} message.
         *
         * @param server       ID of the server.
         * @param numAvailable Number of tickets available on the server.
         */
        public MsgAvailableServer(final Server server, final int numAvailable) {
            this.server = server;
            this.numAvailable = numAvailable;
        }

        @Override
        public void execute(final Estimator est) {
            throw new RuntimeException("Not implemented!");
        }
    }
}
