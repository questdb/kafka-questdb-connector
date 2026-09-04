package io.questdb.kafka;

import io.questdb.client.Sender;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BufferingSenderTest {
    @Test
    void delegatesQwpAcknowledgementMethods() {
        Invocation invocation = new Invocation();
        Sender delegate = (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class<?>[]{Sender.class},
                (proxy, method, args) -> {
                    invocation.method = method.getName();
                    invocation.args = args;
                    switch (method.getName()) {
                        case "flushAndGetSequence":
                            return 42L;
                        case "getAckedFsn":
                            return 40L;
                        case "awaitAckedFsn":
                            return true;
                        case "drain":
                            return false;
                        default:
                            return null;
                    }
                });
        BufferingSender sender = new BufferingSender(delegate, "sym");

        assertEquals(42L, sender.flushAndGetSequence());
        assertEquals("flushAndGetSequence", invocation.method);
        assertEquals(40L, sender.getAckedFsn());
        assertEquals("getAckedFsn", invocation.method);
        assertTrue(sender.awaitAckedFsn(37L, 123L));
        assertEquals("awaitAckedFsn", invocation.method);
        assertEquals(37L, invocation.args[0]);
        assertEquals(123L, invocation.args[1]);
        assertFalse(sender.drain(456L));
        assertEquals("drain", invocation.method);
        assertEquals(456L, invocation.args[0]);
    }

    private static final class Invocation {
        private Object[] args;
        private String method;
    }
}
