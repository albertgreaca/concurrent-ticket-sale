package com.pseuco.cp24.request;

import java.util.UUID;

import com.sun.net.httpserver.HttpExchange;

/**
 * <p>
 * A <em>customer ID</em> uniquely identifies a customer.
 * </p>
 *
 * <p>
 * 📌 Hint: The framework we provide takes care of generating and assigning customer IDs.
 * </p>
 */
public class CustomerId {
    /**
     * Name of the HTTP header for the customer ID.
     */
    protected static final String HEADER_NAME = "X-Customer-Id";

    /**
     * Underlying UUID of the customer.
     */
    private final UUID id;

    /**
     * Generates a new random customer ID.
     *
     * @return Generated customer ID.
     */
    public static CustomerId generate() {
        return new CustomerId(UUID.randomUUID());
    }

    /**
     * Extracts a customer ID from an {@link HttpExchange} or assigns a random one.
     *
     * @param exchange {@link HttpExchange} to extract the ID from.
     * @return Extracted or generated customer ID.
     */
    protected static CustomerId fromHttpExchange(final HttpExchange exchange) {
        CustomerId customerId = null;
        for (var entry : exchange.getRequestHeaders().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(CustomerId.HEADER_NAME)) {
                for (var value : entry.getValue()) {
                    try {
                        customerId = new CustomerId(UUID.fromString(value));
                    } catch (IllegalArgumentException error) {
                        // Client sent an invalid UUID – assign them a new one.
                        System.err.println("Warning: Received an invalid customer ID!");
                    }
                    break;
                }
            }
            if (customerId != null) {
                break;
            }
        }
        if (customerId == null) {
            // Customer does not have an ID yet, so we generate one.
            customerId = CustomerId.generate();
        }
        // Set the response header such that the client receives its customer ID.
        exchange.getResponseHeaders().set(CustomerId.HEADER_NAME, customerId.id.toString());
        return customerId;
    }

    /**
     * Constructs a customer ID from its underlying {@link UUID}.
     *
     * @param id Underlying UUID.
     */
    protected CustomerId(final UUID id) {
        this.id = id;
    }

    /**
     * Returns the UUID of the customer.
     *
     * @return UUID of the customer.
     */
    public UUID getUUID() {
        return this.id;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (this.getClass() != other.getClass()) {
            return false;
        }
        return this.id.equals(((CustomerId) other).id);
    }

    @Override
    public String toString() {
        return String.format("CustomerId(%s)", this.id);
    }
}
