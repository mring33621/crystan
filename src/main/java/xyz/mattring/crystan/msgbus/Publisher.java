package xyz.mattring.crystan.msgbus;

import io.nats.client.Connection;

import java.util.function.Function;

public interface Publisher<T> {

    default void publish(T msg, Function<T, byte[]> msgTransformer, String topic, Connection conn) {
        conn.publish(topic, msgTransformer.apply(msg));
    }

}
