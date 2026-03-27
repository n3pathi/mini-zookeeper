package org.example.rpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class IdempotentCache<T> implements AutoCloseable {

    private final long ttlMs;
    private final Map<String, Wrapper<T>> cache;
    private final ScheduledExecutorService cacheCleaner;

    public IdempotentCache() {
        this(60);
    }

    public IdempotentCache(int ttlSec) {
        this.ttlMs = Duration.of(ttlSec, ChronoUnit.SECONDS).toMillis();
        cache = new ConcurrentHashMap<>();
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("IdempotentCacheCleaner-%d")
                .build();
        cacheCleaner = Executors.newSingleThreadScheduledExecutor(threadFactory);
        Runnable cacheCleanTask = () -> cache.entrySet().removeIf(e -> e.getValue().expired(ttlMs));
        cacheCleaner.scheduleWithFixedDelay(cacheCleanTask, ttlMs, ttlMs, TimeUnit.MILLISECONDS);
    }

    public Optional<T> get(String key) {
        Wrapper<T> orDefault = cache.getOrDefault(key, null);
        if (orDefault != null) {
            if (!orDefault.expired(ttlMs)) {
                return Optional.of(orDefault.value);
            }
            cache.remove(key);
        }
        return Optional.empty();
    }

    public T computeIfAbsent(String key, Supplier<T> supplier) {
        return cache.compute(key, (_, existing) -> {
            if (existing == null || existing.expired(ttlMs)) {
                return new Wrapper<>(supplier.get(), Instant.now().toEpochMilli());
            }
            return existing;
        }).value();
    }

    @Override
    public void close() {
        cacheCleaner.shutdownNow();
    }

    private record Wrapper<T>(T value, long cTime) {
        public boolean expired(long ttlMs) {
            return Instant.ofEpochMilli(cTime).plusMillis(ttlMs).isBefore(Instant.now());
        }
    }
}