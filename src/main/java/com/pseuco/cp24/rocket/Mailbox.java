package com.pseuco.cp24.rocket;

/**
 * <p>
 * A channel for messages of type {@code M} with two priorities.
 * </p>
 * 
 * @param <M> Message type.
 */
public class Mailbox<M> {
    /**
     * Constructs a new empty {@link Mailbox}.
     */
    public Mailbox() {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * Returns whether the mailbox is empty.
     * 
     * @return Whether the mailbox is empty.
     */
    public boolean isEmpty() {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * Tries to send a message with low priority.
     * 
     * @param message The message.
     * @return Indicates whether the message has been sent.
     */
    public boolean sendLowPriority(M message) {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * Ties to send a message with high priority.
     * 
     * @param message The message.
     * @return Indicates whether the message has been sent.
     */
    public boolean sendHighPriority(M message) {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * <p>
     * Receives a message blocking the receiving thread.
     * </p>
     * 
     * <p>
     * 📌 Hint: This is useful for the {@link Server}
     * </p>
     * 
     * @return The received message.
     * @throws InterruptedException The thread has been interrupted.
     */
    public M recv() throws InterruptedException {
        throw new RuntimeException("Not implemented!");
    }

    /**
     * <p>
     * Tries to receive a message without blocking.
     * </p>
     * 
     * <p>
     * 📌 Hint: This is useful for the {@link Estimator}.
     * </p>
     * 
     * @return The received message or {@code null} in case the {@link Mailbox} is empty.
     */
    public M tryRecv() {
        throw new RuntimeException("Not implemented!");
    }
}
