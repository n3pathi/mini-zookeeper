package org.example.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RpcSerializer {
    public static byte[] encodeRequest(RpcRequest original) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            MessageFramer.write(bos, original.id().getBytes(StandardCharsets.UTF_8));
            MessageFramer.write(bos, original.method().getBytes(StandardCharsets.UTF_8));
            List<String> args = original.args();
            MessageFramer.write(bos, ByteBuffer.allocate(4).putInt(args.size()).array());
            for (String arg : args) {
                MessageFramer.write(bos, arg.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bos.toByteArray();
    }

    public static RpcRequest decodeRequest(byte[] encoded) {
        ByteArrayInputStream bais = new ByteArrayInputStream(encoded);
        try {
            String id = new String(MessageFramer.read(bais), StandardCharsets.UTF_8);
            String method = new String(MessageFramer.read(bais), StandardCharsets.UTF_8);
            List<String> args = new ArrayList<>();
            int argLength = ByteBuffer.wrap(MessageFramer.read(bais)).getInt();
            for (int i = 0; i < argLength; i++) {
                args.add(new String(MessageFramer.read(bais), StandardCharsets.UTF_8));
            }
            return new RpcRequest(id, method, args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] encodeResponse(RpcResponse original) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            MessageFramer.write(bos, original.id().getBytes(StandardCharsets.UTF_8));
            MessageFramer.write(bos, original.result().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bos.toByteArray();
    }

    public static RpcResponse decodeResponse(byte[] encoded) {
        ByteArrayInputStream bais = new ByteArrayInputStream(encoded);
        try {
            String id = new String(MessageFramer.read(bais), StandardCharsets.UTF_8);
            String result = new String(MessageFramer.read(bais), StandardCharsets.UTF_8);
            return new RpcResponse(id, result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
