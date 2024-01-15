package xyz.mattring.crystan.cache;

import java.util.Map;
import java.util.function.Function;

/**
 * Very simple cache for things like Nats connections, Jackson mappers, etc.
 */
public class PartsCache {
    private static final Map<String, Object> cache = new java.util.concurrent.ConcurrentHashMap<>();

    public static Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
        return cache.computeIfAbsent(key, mappingFunction);
    }
}
