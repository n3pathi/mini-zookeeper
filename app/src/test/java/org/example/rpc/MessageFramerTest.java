package org.example.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MessageFramer — length-prefixed message framing over a byte stream.
 *
 * Wire format:  [4-byte big-endian length][payload bytes]
 *
 * Why length-prefixed? TCP is a stream — there are no message boundaries.
 * Without framing, a reader cannot know where one message ends and the next begins.
 */
class MessageFramerTest {

    @Test
    void writeAndRead_roundTrip_singleMessage() throws IOException {
        byte[] payload = "hello".getBytes();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageFramer.write(baos, payload);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        byte[] result = MessageFramer.read(bais);

        assertArrayEquals(payload, result);
    }

    @Test
    void writeAndRead_roundTrip_multipleMessages() throws IOException {
        byte[] msg1 = "first".getBytes();
        byte[] msg2 = "second".getBytes();
        byte[] msg3 = "third".getBytes();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageFramer.write(baos, msg1);
        MessageFramer.write(baos, msg2);
        MessageFramer.write(baos, msg3);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        assertArrayEquals(msg1, MessageFramer.read(bais));
        assertArrayEquals(msg2, MessageFramer.read(bais));
        assertArrayEquals(msg3, MessageFramer.read(bais));
    }

    @Test
    void write_prefixesCorrectLength() throws IOException {
        byte[] payload = "hi".getBytes(); // 2 bytes

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageFramer.write(baos, payload);

        byte[] raw = baos.toByteArray();
        // First 4 bytes = length prefix
        int length = ((raw[0] & 0xFF) << 24) | ((raw[1] & 0xFF) << 16)
                   | ((raw[2] & 0xFF) << 8)  |  (raw[3] & 0xFF);
        assertEquals(2, length);
    }

    @Test
    void read_throwsIOException_onEmptyStream() {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        assertThrows(IOException.class, () -> MessageFramer.read(bais));
    }

    @Test
    void read_throwsIOException_onTruncatedPayload() throws IOException {
        byte[] payload = "hello world".getBytes();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageFramer.write(baos, payload);

        // Truncate — only write half the bytes
        byte[] truncated = new byte[baos.size() - 5];
        System.arraycopy(baos.toByteArray(), 0, truncated, 0, truncated.length);

        ByteArrayInputStream bais = new ByteArrayInputStream(truncated);
        assertThrows(IOException.class, () -> MessageFramer.read(bais));
    }

    @Test
    void write_handlesEmptyPayload() throws IOException {
        byte[] payload = new byte[0];

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageFramer.write(baos, payload);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        assertArrayEquals(payload, MessageFramer.read(bais));
    }
}
