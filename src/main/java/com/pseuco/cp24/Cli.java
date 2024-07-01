package com.pseuco.cp24;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.pseuco.cp24.rocket.Rocket;
import com.pseuco.cp24.slug.Slug;
import com.sun.net.httpserver.HttpServer;

/**
 * <p>
 * Implements the <em>main</em> method and the command line interface.
 * </p>
 *
 * <p>
 * 📌 For the most part, you can ignore this class.
 * </p>
 */
public class Cli {
    /**
     * Explicit default constructor.
     */
    protected Cli() {
    }

    /**
     * Port for the HTTP server to listen on.
     */
    @Parameter(names = "-port")
    private int port = 8585;

    /**
     * Hostname for the HTTP server to listen on.
     */
    @Parameter(names = "-host")
    private String host = "127.0.0.1";

    /**
     * Amount of tickets initially available.
     */
    @Parameter(names = "-tickets")
    private int tickets = 1000;

    /**
     * Timeout after which reservations expire in seconds.
     */
    @Parameter(names = "-timeout")
    private int timeout = 10;

    /**
     * Time in seconds the estimator takes to contact all servers.
     */
    @Parameter(names = "-estimator-roundtrip-time")
    private int estimatorRoundtripTime = 10;

    /**
     * <p>
     * Number of threads of the load balancer.
     * </p>
     *
     * <p>
     * 📌 Hint: This is handled by the provided framework.
     * </p>
     */
    @Parameter(names = "-balancer-threads")
    private int balancerThreads = 64;

    /**
     * Run the slow “slug” implementation. 🐌
     */
    @Parameter(names = "-slug")
    private boolean slug = false;

    /**
     * Run the implementation for the bonus exercise.
     */
    @Parameter(names = "-bonus")
    private boolean bonus = false;

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     * @throws IOException When there is an I/O error.
     */
    public static void main(final String[] args) throws IOException {
        final var app = new Cli();
        JCommander.newBuilder().addObject(app).args(args).build();
        app.run();
    }

    /**
     * Runs the ticket sales system.
     *
     * @throws IOException When there is an I/O error.
     */
    public void run() throws IOException {
        final var options = new Config(this.tickets, this.timeout, this.estimatorRoundtripTime, this.bonus);
        // Create the handler for requests and the HTTP server.
        final var handler = this.slug ? new Slug(options) : Rocket.launch(options);
        final var server = HttpServer.create(new InetSocketAddress(this.host, this.port), 8);
        // We are doing all the routing of requests ourselves.
        server.createContext("/", new ExchangeHandler(handler));
        // Use a fixed thread pool as an executor for load balancing.
        server.setExecutor(Executors.newFixedThreadPool(this.balancerThreads));
        // Start the HTTP server.
        server.start();
    }
}
