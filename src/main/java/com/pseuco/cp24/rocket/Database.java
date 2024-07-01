package com.pseuco.cp24.rocket;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the central database for tickets.
 */
public class Database {
    /**
     * Tickets that are currently in the database.
     */
    private final List<Ticket> unallocated = new ArrayList<>();

    /**
     * Constructs a new {@link Database}.
     *
     * @param coordinator The {@link Coordinator} of the ticket sales system.
     */
    public Database(final Coordinator coordinator) {
        // Generate the necessary number of tickets.
        for (var id = 0; id < coordinator.getConfig().getNumTickets(); id++) {
            this.unallocated.add(new Ticket(id));
        }
    }

    /**
     * Returns the number of tickets available in the database.
     *
     * @return Number of tickets available in the database.
     */
    public int getNumAvailable() {
        return unallocated.size();
    }

    /**
     * <p>
     * Tries to allocate at most the given number of tickets.
     * </p>
     *
     * <p>
     * 📌 Hint: Return an empty list in case the database has no tickets left.
     * </p>
     *
     * @param numTickets Number of tickets to allocate.
     * @return A list of allocated tickets.
     */
    public List<Ticket> allocate(final int numTickets) {
        final var num = Math.min(numTickets, unallocated.size());
        final var tickets = new ArrayList<Ticket>(num);
        for (var i = 0; i < num; i++) {
            tickets.add(unallocated.remove(unallocated.size() - 1));
        }
        return tickets;
    }

    /**
     * Deallocates previously allocated tickets.
     *
     * @param tickets Tickets to return to the database.
     */
    public void deallocate(final Iterable<Ticket> tickets) {
        for (final var ticket : tickets) {
            unallocated.add(ticket);
        }
    }
}
