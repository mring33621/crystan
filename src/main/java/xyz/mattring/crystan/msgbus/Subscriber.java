package xyz.mattring.crystan.msgbus;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import xyz.mattring.crystan.util.Tuple2;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Subscriber<T> {

    default Tuple2<Dispatcher, Subscription> subscribe(Consumer<T> msgConsumer, Function<byte[], T> msgTransformer, String topic, Connection conn) {
        final Dispatcher dispatcher = conn.createDispatcher((msg) -> {
        });
        final Subscription subscription = dispatcher.subscribe(topic, (msg) -> {
            msgConsumer.accept(msgTransformer.apply(msg.getData()));
        });
        return new Tuple2<>(dispatcher, subscription);
    }

}
