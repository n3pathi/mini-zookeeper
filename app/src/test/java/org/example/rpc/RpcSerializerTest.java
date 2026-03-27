package org.example.rpc;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RpcSerializer — custom binary encode/decode for RpcRequest and RpcResponse.
 *
 * Wire format for RpcRequest:
 *   [id length (4 bytes)][id bytes][method length (4 bytes)][method bytes]
 *   [arg count (4 bytes)] for each arg: [arg length (4 bytes)][arg bytes]
 *
 * Wire format for RpcResponse:
 *   [id length (4 bytes)][id bytes][result length (4 bytes)][result bytes]
 */
class RpcSerializerTest {

    // -------------------------------------------------------------------------
    // RpcRequest serialization
    // -------------------------------------------------------------------------

    @Test
    void encodeDecodeRequest_roundTrip_withArgs() {
        RpcRequest original = new RpcRequest(
                UUID.randomUUID().toString(),
                "add",
                List.of("10", "20")
        );

        byte[] encoded = RpcSerializer.encodeRequest(original);
        RpcRequest decoded = RpcSerializer.decodeRequest(encoded);

        assertEquals(original.id(), decoded.id());
        assertEquals(original.method(), decoded.method());
        assertEquals(original.args(), decoded.args());
    }

    @Test
    void encodeDecodeRequest_roundTrip_noArgs() {
        RpcRequest original = new RpcRequest(
                UUID.randomUUID().toString(),
                "ping",
                List.of()
        );

        byte[] encoded = RpcSerializer.encodeRequest(original);
        RpcRequest decoded = RpcSerializer.decodeRequest(encoded);

        assertEquals(original.id(), decoded.id());
        assertEquals(original.method(), decoded.method());
        assertTrue(decoded.args().isEmpty());
    }

    @Test
    void encodeDecodeRequest_roundTrip_manyArgs() {
        List<String> args = List.of("1", "2", "3", "4", "5");
        RpcRequest original = new RpcRequest(UUID.randomUUID().toString(), "sum", args);

        RpcRequest decoded = RpcSerializer.decodeRequest(RpcSerializer.encodeRequest(original));

        assertEquals(args, decoded.args());
    }

    // -------------------------------------------------------------------------
    // RpcResponse serialization
    // -------------------------------------------------------------------------

    @Test
    void encodeDecodeResponse_roundTrip() {
        RpcResponse original = new RpcResponse(UUID.randomUUID().toString(), "42");

        byte[] encoded = RpcSerializer.encodeResponse(original);
        RpcResponse decoded = RpcSerializer.decodeResponse(encoded);

        assertEquals(original.id(), decoded.id());
        assertEquals(original.result(), decoded.result());
    }

    @Test
    void encodeDecodeResponse_roundTrip_longResult() {
        String longResult = "x".repeat(10_000);
        RpcResponse original = new RpcResponse(UUID.randomUUID().toString(), longResult);

        RpcResponse decoded = RpcSerializer.decodeResponse(RpcSerializer.encodeResponse(original));

        assertEquals(longResult, decoded.result());
    }

    // -------------------------------------------------------------------------
    // Determinism
    // -------------------------------------------------------------------------

    @Test
    void encode_isDeterministic_sameInputProducesSameBytes() {
        RpcRequest req = new RpcRequest("fixed-id", "add", List.of("1", "2"));

        byte[] first = RpcSerializer.encodeRequest(req);
        byte[] second = RpcSerializer.encodeRequest(req);

        assertArrayEquals(first, second);
    }
}
