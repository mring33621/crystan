package xyz.mattring.crystan.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import xyz.mattring.crystan.cache.PartsCache;

public interface JsonConverter {

    default ObjectMapper getObjectMapper() {
        final String cacheKey = "mapper_" + System.identityHashCode(this);
        return (ObjectMapper) PartsCache.computeIfAbsent(
                cacheKey, key -> new ObjectMapper());
    }

    default <T> ObjectReader getObjectReader(Class<T> clazz) {
        final String cacheKey = clazz.getSimpleName() + "_reader_" + System.identityHashCode(this);
        return (ObjectReader) PartsCache.computeIfAbsent(
                cacheKey, key -> getObjectMapper().readerFor(clazz));
    }

    default ObjectWriter getObjectWriter() {
        final String cacheKey = "writer_" + System.identityHashCode(this);
        return (ObjectWriter) PartsCache.computeIfAbsent(
                cacheKey, key -> getObjectMapper().writer().withDefaultPrettyPrinter());
    }

    default <T> T fromJson(String json, Class<T> clazz) {
        try {
            return getObjectReader(clazz).readValue(json);
        } catch (JsonProcessingException jpex) {
            throw new RuntimeException(jpex);
        }
    }

    default String toJson(Object obj) {
        try {
            return getObjectWriter().writeValueAsString(obj);
        } catch (JsonProcessingException jpex) {
            throw new RuntimeException(jpex);
        }
    }

}
