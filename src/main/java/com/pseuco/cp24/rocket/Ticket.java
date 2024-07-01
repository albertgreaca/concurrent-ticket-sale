package com.pseuco.cp24.rocket;

import java.util.ArrayList;

/**
 * Represents a ticket and its state transitions.
 */
public class Ticket {
    /**
     * Represents the state of a ticket.
     */
    public static enum State {
        /**
         * The ticket is <em>available</em>, i.e., it has neither been reserved nor sold.
         */
        AVAILABLE,
        /**
         * The ticket has been <em>reserved</em> by a customer.
         */
        RESERVED,
        /**
         * The ticket has been <em>sold</em> to a customer.
         */
        SOLD;
    }

    /**
     * Generates a stack of <em>numTickets</em> tickets.
     *
     * @param numTickets Amount of tickets to generate.
     * @return A stack of tickets.
     */
    public static ArrayList<Ticket> generateStack(final int numTickets) {
        final var tickets = new ArrayList<Ticket>(numTickets);
        for (var id = 0; id < numTickets; id++) {
            tickets.add(new Ticket(id));
        }
        return tickets;
    }

    /**
     * ID of the ticket.
     */
    private final int id;

    /**
     * State of the ticket.
     */
    private State state = State.AVAILABLE;

    /**
     * Constructs a new ticket with the provided <em>ID</em>.
     *
     * @param id ID of the ticket.
     */
    public Ticket(final int id) {
        this.id = id;
    }

    /**
     * Returns the ID of the ticket.
     *
     * @return ID of the ticket.
     */
    public int getId() {
        return this.id;
    }

    /**
     * Returns the state of the ticket.
     *
     * @return State of the ticket.
     */
    public State getState() {
        return this.state;
    }

    /**
     * <em>Reserves</em> the ticket.
     */
    public void reserve() {
        assert this.state == State.AVAILABLE : "Ticket is not available!";
        this.state = State.RESERVED;
    }

    /**
     * <em>Aborts</em> the reservation of the ticket.
     */
    public void abort() {
        assert this.state == State.RESERVED : "Ticket is not reserved!";
        this.state = State.AVAILABLE;
    }

    /**
     * <em>Sells</em> the ticket.
     */
    public void sell() {
        assert this.state == State.RESERVED : "Ticket is not reserved!";
        this.state = State.SOLD;
    }
}
