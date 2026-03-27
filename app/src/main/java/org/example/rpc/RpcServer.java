package org.example.rpc;

import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Log4j2
public class RpcServer implements AutoCloseable {
    public static final Function<List<String>, String> NO_OP = (_) -> "ERROR: unknown method";

    private final ServerSocket serverSocket;
    private final IdempotentCache<String> cache;
    private final ConcurrentHashMap<String, Function<List<String>, String>> handlers;
    private final ExecutorService service;
    private final AtomicBoolean running;
    private final Set<Socket> activeConnections = ConcurrentHashMap.newKeySet();

    public RpcServer(int port) {
        try {
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
            serverSocket = ss;
            log.info("Server started on port {}\n", serverSocket.getLocalPort());
        } catch (IOException e) {
            throw new RuntimeException("port %d".formatted(port), e);
        }
        this.cache = new IdempotentCache<>();
        handlers = new ConcurrentHashMap<>();
        service = Executors.newVirtualThreadPerTaskExecutor();
        running = new AtomicBoolean(false);
    }

    public void start() {
        running.set(true); // start
        service.submit(() -> {
            try {
                while (running.get()) {
                    Socket client = serverSocket.accept();
                    client.setTcpNoDelay(true);
                    client.setKeepAlive(true);
                    activeConnections.add(client);

                    Thread.ofVirtual().start(() -> {
                        try {
                            while (!client.isClosed()) {
//                                keep serving the client request from the same thread, util closed
                                handleClientRequests(client);
                            }
                        } catch (IOException e) {
                            log.error("error accepting client connection: {}", e.getMessage());
                        } finally {
                            activeConnections.remove(client);
                        }
                    });
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void handleClientRequests(Socket client) throws IOException {
        byte[] encoded = MessageFramer.read(client.getInputStream());
        RpcRequest request = RpcSerializer.decodeRequest(encoded);
        String result = cache.computeIfAbsent(request.id(),
                () -> {
                    Function<List<String>, String> fn = handlers.getOrDefault(request.method(), NO_OP);
                    int maxAttempts = 5;
                    int attempts = 1;
                    while (attempts < maxAttempts) {
                        try {
                            return fn.apply(request.args());
                        } catch (RetryableException _) {
                            long jitter = ThreadLocalRandom.current().nextInt(10, 50);
                            long delay = jitter + Math.powExact(2, attempts);
                            try {
                                TimeUnit.MILLISECONDS.sleep(delay);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        attempts++;
                    }
                    return null;
                });
        RpcResponse response = new RpcResponse(request.id(), result);
        byte[] encodedResponse = RpcSerializer.encodeResponse(response);
        MessageFramer.write(client.getOutputStream(), encodedResponse);
    }

    @Override
    public void close() {
        log.info("Shut down triggered: server for port = {}", serverSocket.getLocalPort());
        running.set(false);
        for (Socket conn : activeConnections) {
            try { conn.close(); } catch (IOException ignored) {}
        }
        activeConnections.clear();
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
        service.shutdown();
        log.info("Shutdown complete: server for port = {}", serverSocket.getLocalPort());
    }

    public void register(String method, Function<List<String>, String> handler) {
        log.info("method = {}, handler = {}", method, handler);
        handlers.put(method, handler);
    }
}
