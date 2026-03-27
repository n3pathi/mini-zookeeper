package org.example.rpc;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Log4j2
public class RpcClient {
    @Getter
    private final String host;
    @Getter
    private final int port;
    private final int ttlMs;
    private final ConcurrentHashMap<String, CompletableFuture<RpcResponse>> pending;
    private volatile Socket socket;
    private volatile ExecutorService responseReader;
    private final Object connectLock = new Object();

    public RpcClient(String host, int port) {
        this(host, port, 2000);
    }

    public RpcClient(String host, int port, int ttlMs) {
        this.host = host;
        this.port = port;
        this.ttlMs = ttlMs;
        pending = new ConcurrentHashMap<>();
    }

    private void ensureConnected() throws IOException {
        if (isSocketConnected()) {
            return;
        }
        synchronized (connectLock) {
            if (isSocketConnected()) {
                return;
            }
            Socket s = new Socket();
            s.connect(new InetSocketAddress(host, port));
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            socket = s;
            responseReader = Executors.newSingleThreadExecutor();
            responseReader.submit(() -> {
                while (!socket.isClosed()) {
                    try {
                        byte[] payload = MessageFramer.read(socket.getInputStream());
                        RpcResponse response = RpcSerializer.decodeResponse(payload);
                        CompletableFuture<RpcResponse> future = pending.remove(response.id());
                        if (future != null) {
                            future.complete(response);
                        }
                    } catch (IOException e) {
                        // fatal: connection broken
                        pending.values().forEach(f -> f.completeExceptionally(e));
                        pending.clear();
                        try {
                            socket.close();
                        } catch (IOException ignored) {
                        }
                        break;
                    } catch (Exception e) {
                        // Should never happen — diagnose
                        log.error("[READER DIED]: {}: {}", e.getClass().getName(), e.getMessage());
                        pending.values().forEach(f -> f.completeExceptionally(e));
                        pending.clear();
                        try {
                            socket.close();
                        } catch (IOException ignored) {
                        }
                        break;
                    }
                }
            });
        }
    }

    private boolean isSocketConnected() {
        return socket != null && !socket.isClosed() && socket.isConnected();
    }

    public void close() {
        if (socket != null) {
            try {
                if (responseReader != null) {
                    responseReader.shutdown();
                }
                pending.clear();
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public CompletableFuture<RpcResponse> call(String method, List<String> args) {
        return callWithId(UUID.randomUUID().toString(), method, args);
    }

    public CompletableFuture<RpcResponse> callWithId(String id, String method, List<String> args) {
        try {
            ensureConnected();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RpcRequest request = new RpcRequest(id, method, args);
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        pending.put(request.id(), future);

        try {
            OutputStream cos = socket.getOutputStream();
            synchronized (cos) {
                byte[] encoded = RpcSerializer.encodeRequest(request);
                MessageFramer.write(socket.getOutputStream(), encoded);
                cos.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return future.orTimeout(ttlMs, TimeUnit.MILLISECONDS);
    }

}
