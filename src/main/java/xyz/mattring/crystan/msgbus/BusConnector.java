package xyz.mattring.crystan.msgbus;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import xyz.mattring.crystan.cache.PartsCache;

import java.io.IOException;

public interface BusConnector {

    default Options getOptions() {
        return new Options.Builder().build();
    }

    default Connection getConnection() {
        final String cacheKey = "conn_" + this.hashCode();
        return (Connection) PartsCache.computeIfAbsent(cacheKey, key -> {
            try {
                return Nats.connect(getOptions());
            } catch (IOException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
    }
}
