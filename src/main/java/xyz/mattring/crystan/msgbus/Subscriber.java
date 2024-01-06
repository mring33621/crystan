package xyz.mattring.crystan.msgbus;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Subscriber<T> {

    default Subscription subscribe(Consumer<T> msgConsumer, Function<byte[], T> msgTransformer, String topic, Connection conn) {
        Dispatcher dispatcher = conn.createDispatcher((msg) -> {
        });
        return dispatcher.subscribe(topic, (msg) -> {
            msgConsumer.accept(msgTransformer.apply(msg.getData()));
        });
    }

}
