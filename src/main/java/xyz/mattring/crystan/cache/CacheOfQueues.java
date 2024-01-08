package xyz.mattring.crystan.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class CacheOfQueues<T> {
    private final Cache<String, Queue<T>> cache = Caffeine.newBuilder()
            .expireAfterAccess(2, TimeUnit.MINUTES)
            .build();

    public void offer(String cacheKey, T value) {
        final Queue<T> q = cache.get(cacheKey, key -> new java.util.concurrent.ConcurrentLinkedQueue<>());
        q.offer(value);
    }

    public T poll(String cacheKey) {
        final Queue<T> q = cache.get(cacheKey, null);
        if (q == null) {
            return null;
        }
        return q.poll();
    }
}
