package xyz.mattring.crystan.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import xyz.mattring.crystan.cache.PartsCache;

public interface JsonConverter {

    default ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

    default <T> ObjectReader getObjectReader(Class<T> clazz) {
        final String cacheKey = "reader_" + this.hashCode();
        return (ObjectReader) PartsCache.computeIfAbsent(
                cacheKey, key -> getObjectMapper().readerFor(clazz));
    }

    default ObjectWriter getObjectWriter() {
        final String cacheKey = "writer_" + this.hashCode();
        return (ObjectWriter) PartsCache.computeIfAbsent(
                cacheKey, key -> getObjectMapper().writer().withDefaultPrettyPrinter());
    }

    default <T> T fromJson(String json, Class<T> clazz) {
        try {
            return getObjectReader(clazz).readValue(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    default String toJson(Object obj) {
        try {
            return getObjectWriter().writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
