package xyz.mattring.crystan.service;

import org.junit.jupiter.api.Test;
import xyz.mattring.crystan.json.JsonConverter;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.util.BytesConverter;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublisherSingleThreadServerIT extends NatsTestBase {

    @Test
    public void testPubSub() throws IOException, InterruptedException {
        final String testSubjectA = "it.test.subjectA";
        final String testSubjectB = "it.test.subjectB";
        final String jobId = "jobId-999";
        final Foo testFoo = new Foo("foo", 1.0d);
        final Bar testBar = new Bar(testFoo);
        final BusConnector busConnector = new BusConnector() {
        };
        final Publisher<Foo> publisher = new Publisher<Foo>() {
        };
        final JsonConverter jsonConverter = new JsonConverter() {
        };
        final CountDownLatch recvdMsgLatch = new CountDownLatch(1);
        SingleThreadServer<Foo, Bar> server = new SingleThreadServer<>(testSubjectA, testSubjectB, Foo.class, foo -> {
            System.out.println("received " + foo);
            assertEquals(testFoo, foo);
            Bar resp = new Bar(foo);
            System.out.println("sending " + resp);
            assertEquals(testBar, resp);
            recvdMsgLatch.countDown();
            return resp;
        });

        Thread serverThread = new Thread(server);
        serverThread.start();

        publisher.publish(testFoo,
                msg -> {
                    return JobIdSerdeHelper.prependPayloadWithJobId(
                            jobId,
                            BytesConverter.utf8ToBytes(jsonConverter.toJson(msg)));
                },
                testSubjectA,
                busConnector.getConnection());
        boolean recvdMsg = recvdMsgLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        server.stop();
        assertTrue(recvdMsg);
    }

}
