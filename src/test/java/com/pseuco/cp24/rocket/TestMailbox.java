package com.pseuco.cp24.rocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class TestMailbox {
    @Test(timeout = 10000)
    public void testPriorities() throws InterruptedException {
        final var mailbox = new Mailbox<String>();

        mailbox.sendHighPriority("1. High");
        mailbox.sendLowPriority("1. Low");
        mailbox.sendHighPriority("2. High");
        mailbox.sendLowPriority("2. Low");

        assertEquals("1. High", mailbox.recv());
        assertEquals("2. High", mailbox.recv());
        assertEquals("1. Low", mailbox.recv());
        assertEquals("2. Low", mailbox.recv());
    }

    @Test(timeout = 10000)
    public void testTryRecv() {
        final var mailbox = new Mailbox<String>();

        assertNull(mailbox.tryRecv());

        mailbox.sendLowPriority("Low");
        mailbox.sendHighPriority("High");

        assertEquals("High", mailbox.tryRecv());
        assertEquals("Low", mailbox.tryRecv());

        assertNull(mailbox.tryRecv());
    }
}
