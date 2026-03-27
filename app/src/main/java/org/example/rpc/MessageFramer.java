package org.example.rpc;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MessageFramer {
    public static void write(OutputStream os, byte[] payload) throws IOException {
        os.write(ByteBuffer.allocate(4).putInt(payload.length).array());
        os.write(payload);
    }

    public static byte[] read(InputStream is) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(is.readNBytes(4));
        if (buffer.remaining() < 4) {
            throw new EOFException("empty stream");
        }
        int len = buffer.getInt();
        byte[] payload = is.readNBytes(len);
        if (len != payload.length) {
            throw new IOException("invalid stream length: expected %d but found %d".formatted(len, payload.length));
        }
        return payload;
    }
}
