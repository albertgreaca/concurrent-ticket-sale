package com.pseuco.cp24.slug;

/**
 * Represents a reservation of a ticket by a specific customer.
 */
public class Reservation {
    /**
     * Reserved ticket.
     */
    private final Ticket ticket;

    /**
     * System time at which the ticket has been reserved.
     *
     * This is necessary for determining when a reservation's timeout has expired.
     */
    private final long reservedAt;

    /**
     * Constructs a new reservation.
     *
     * @param ticket Ticket that should be reserved.
     */
    public Reservation(final Ticket ticket) {
        ticket.reserve();
        this.ticket = ticket;
        this.reservedAt = System.currentTimeMillis();
    }

    /**
     * Returns the ID of the reserved ticket.
     *
     * @return ID of the reserved ticket.
     */
    public int getTicketId() {
        return this.ticket.getId();
    }

    /**
     * Returns the age of the reservation in seconds.
     *
     * @return Age of the reservation in seconds.
     */
    public long getAge() {
        return (System.currentTimeMillis() - this.reservedAt) / 1000;
    }

    /**
     * Aborts the reservation and returns the ticket.
     *
     * @return Ticket associated with the reservation.
     */
    public Ticket abort() {
        this.ticket.abort();
        return this.ticket;
    }

    /**
     * Marks the ticket as sold and returns it.
     *
     * @return Ticket associated with the reservation.
     */
    public Ticket sell() {
        this.ticket.sell();
        return this.ticket;
    }
}
