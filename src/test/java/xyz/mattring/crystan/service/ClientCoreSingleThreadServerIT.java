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

public class ClientCoreSingleThreadServerIT extends NatsTestBase {

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
        final CountDownLatch recvdMsgLatch = new CountDownLatch(2); // server recv + client recv
        SingleThreadServer<Foo, Bar> server = new SingleThreadServer<>(testSubjectA, testSubjectB, Foo.class, foo -> {
            System.out.println("server received " + foo);
            assertEquals(testFoo, foo);
            Bar resp = new Bar(foo);
            System.out.println("server sending " + resp);
            assertEquals(testBar, resp);
            recvdMsgLatch.countDown();
            return resp;
        });

        Thread serverThread = new Thread(server);
        serverThread.start();

        ClientCore<Foo, Bar> clientCore = new ClientCore<>(
                testSubjectA,
                testSubjectB,
                foo -> { // Foo to byte[]
                    System.out.println("client sending " + foo);
                    assertEquals(testFoo, foo);
                    return BytesConverter.utf8ToBytes(jsonConverter.toJson(foo));
                },
                bytes -> { // byte[] to Bar
                    Bar resp = jsonConverter.fromJson(BytesConverter.bytesToUtf8(bytes), Bar.class);
                    System.out.println("client deserialized response " + resp);
                    return resp;
                });
        Thread clientCoreThread = new Thread(clientCore);
        clientCoreThread.start();
        clientCore.sendRequest(testFoo, bar -> {
            System.out.println("client processed response " + bar);
            recvdMsgLatch.countDown();
            assertEquals(testBar, bar);
        });

        boolean recvdMsg = recvdMsgLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        clientCore.stop();
        server.stop();
        assertEquals(0, clientCore.getNumHandlers(), "clientCore.getNumHandlers() != 0");
        assertTrue(recvdMsg, "recvdMsgLatch.await() timed out");
    }

}
