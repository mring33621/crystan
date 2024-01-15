package xyz.mattring.crystan.service;

import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import org.junit.jupiter.api.Test;
import xyz.mattring.crystan.json.JsonConverter;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;
import xyz.mattring.crystan.util.BytesConverter;
import xyz.mattring.crystan.util.Tuple2;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublisherSubscriberIT extends NatsTestBase {

    @Test
    public void testPubSub() throws IOException, InterruptedException {
        final String testSubject = "it.test.subject";
        final BusConnector busConnector = new BusConnector() {
        };
        final Publisher<Foo> publisher = new Publisher<Foo>() {
        };
        final Subscriber<Foo> subscriber = new Subscriber<Foo>() {
        };
        final JsonConverter jsonConverter = new JsonConverter() {
        };
        final Foo testFoo = new Foo("foo", 1.0d);
        final CountDownLatch recvdMsgLatch = new CountDownLatch(1);
        final Tuple2<Dispatcher, Subscription> subParts = subscriber.subscribe(
                msg -> {
                    System.out.println("received " + msg);
                    recvdMsgLatch.countDown();
                    assertEquals(testFoo, msg);
                },
                msgBytes -> {
                    return jsonConverter.fromJson(BytesConverter.bytesToUtf8(msgBytes), Foo.class);
                },
                testSubject,
                busConnector.getConnection());
        Thread.sleep(1000L);
        publisher.publish(testFoo,
                msg -> {
                    return BytesConverter.utf8ToBytes(jsonConverter.toJson(msg));
                },
                testSubject,
                busConnector.getConnection());
        boolean recvdMsg = recvdMsgLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        subParts._1().unsubscribe(subParts._2());
        assertTrue(recvdMsg);
    }

}
