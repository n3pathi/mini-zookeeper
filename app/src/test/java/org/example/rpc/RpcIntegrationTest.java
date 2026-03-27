package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RpcServer + RpcClient.
 *
 * These tests start a real server on a local port and drive it via a real client.
 * No mocking — this validates the full request/response path over TCP.
 */
class RpcIntegrationTest {

    private static final int PORT = 9080;

    private RpcServer server;
    private RpcClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new RpcServer(PORT);
        server.register("echo", args -> args.get(0));
        server.register("add", args ->
                String.valueOf(Integer.parseInt(args.get(0)) + Integer.parseInt(args.get(1))));
        server.register("slow", args -> {
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "done";
        });
        server.start();

        client = new RpcClient("localhost", PORT, 100);
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.close();
    }

    // -------------------------------------------------------------------------
    // Basic request / response
    // -------------------------------------------------------------------------

    @Test
    void call_returnsCorrectResult_forRegisteredMethod() throws Exception {
        RpcResponse response = client.call("echo", List.of("hello")).get(2, TimeUnit.SECONDS);
        assertEquals("hello", response.result());
    }

    @Test
    void call_handlesMultipleArgs() throws Exception {
        RpcResponse response = client.call("add", List.of("7", "3")).get(2, TimeUnit.SECONDS);
        assertEquals("10", response.result());
    }

    @Test
    void call_returnsError_forUnknownMethod() throws Exception {
        RpcResponse response = client.call("unknown", List.of()).get(2, TimeUnit.SECONDS);
        assertTrue(response.result().startsWith("ERROR"),
                "Unknown method should return an ERROR result, got: " + response.result());
    }

    @Test
    void call_correlatesResponses_forConcurrentRequests() throws Exception {
        int count = 20;
        var futures = new java.util.ArrayList<java.util.concurrent.CompletableFuture<RpcResponse>>();

        for (int i = 0; i < count; i++) {
            final String val = String.valueOf(i);
            futures.add(client.call("echo", List.of(val)));
        }

        for (int i = 0; i < count; i++) {
            RpcResponse r = futures.get(i).get(3, TimeUnit.SECONDS);
            assertEquals(String.valueOf(i), r.result(),
                    "Response must match its own request — not another concurrent request");
        }
    }

    // -------------------------------------------------------------------------
    // Idempotency — duplicate request ID must not re-execute handler
    // -------------------------------------------------------------------------

    @Test
    void server_returnsCachedResult_forDuplicateRequestId() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        server.register("counted", args -> {
            callCount.incrementAndGet();
            return "result";
        });

        String fixedId = UUID.randomUUID().toString();

        RpcResponse first  = client.callWithId(fixedId, "counted", List.of()).get(2, TimeUnit.SECONDS);
        RpcResponse second = client.callWithId(fixedId, "counted", List.of()).get(2, TimeUnit.SECONDS);

        assertEquals("result", first.result());
        assertEquals("result", second.result());
        assertEquals(1, callCount.get(),
                "Handler must be invoked exactly once for duplicate request IDs");
    }

    // -------------------------------------------------------------------------
    // Retry — client retries on transient failure
    // -------------------------------------------------------------------------

    @Test
    void client_retries_andEventuallySucceeds() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);
        server.register("flaky", args -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RetryableException("transient failure");
            }
            return "success";
        });

        RpcResponse response = client.call("flaky", List.of()).get(5, TimeUnit.SECONDS);
        assertEquals("success", response.result());
        assertEquals(3, attempts.get(), "Handler should have been called 3 times");
    }

    // -------------------------------------------------------------------------
    // Timeout — call must fail fast if server is too slow
    // -------------------------------------------------------------------------

    @Test
    void client_timesOut_ifServerIsTooSlow() {
        // "slow" handler sleeps 200ms; client timeout is set to 100ms
        RpcClient fastTimeoutClient = new RpcClient("localhost", PORT, 100);

        assertThrows(Exception.class, () ->
                fastTimeoutClient.call("slow", List.of()).get(1, TimeUnit.SECONDS));
    }

    // -------------------------------------------------------------------------
    // Load — many sequential requests complete successfully
    // -------------------------------------------------------------------------

    @Test
    void server_handlesHighLoad_withoutErrors() throws Exception {
        int total = 100;
        for (int i = 0; i < total; i++) {
            RpcResponse r = client.call("add", List.of(String.valueOf(i), "1"))
                    .get(2, TimeUnit.SECONDS);
            assertEquals(String.valueOf(i + 1), r.result());
        }
    }
}
