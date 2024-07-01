package com.pseuco.cp24.rocket;

/**
 * <p>
 * Generic interface for the
 * <a href="https://en.wikipedia.org/wiki/Command_pattern">command pattern</a>.
 * </p>
 *
 * <p>
 * 📌 Hint: You do not have to use the command pattern for processing messages.
 * </p>
 * 
 * @param <O> Object type to execute the command on.
 */
public interface Command<O> {
    /**
     * Executes the command on the provided object.
     *
     * @param obj Object to execute the command on.
     */
    void execute(O obj);
}
