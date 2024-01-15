package xyz.mattring.crystan.service;

import org.junit.jupiter.api.Test;
import xyz.mattring.crystan.json.JsonConverter;
import xyz.mattring.crystan.util.BytesConverter;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientCoreServerCoreIT extends NatsTestBase {

    @Test
    public void testPubSub() throws IOException, InterruptedException {
        final String testSubjectA = "it.test.subjectA";
        final String testSubjectB = "it.test.subjectB";
        final String jobId = "jobId-999";
        final Foo testFoo = new Foo("foo", 1.0d);
        final Bar testBar = new Bar(testFoo);
        final JsonConverter jsonConverter = new JsonConverter() {
        };
        final CountDownLatch recvdMsgLatch = new CountDownLatch(2); // server recv + client recv
        ServerCore<Foo, Bar> server = new ServerCore<>(
                testSubjectA,
                testSubjectB,
                msgBytes -> {
                    Foo foo = jsonConverter.fromJson(BytesConverter.bytesToUtf8(msgBytes), Foo.class);
                    System.out.println("server received request " + foo);
                    recvdMsgLatch.countDown();
                    assertEquals(testFoo, foo);
                    return foo;
                },
                fooMsg -> {
                    Bar resp = new Bar(fooMsg);
                    System.out.println("server built " + resp);
                    return resp;
                },
                barMsg -> {
                    System.out.println("server sending " + barMsg);
                    assertEquals(testBar, barMsg);
                    return BytesConverter.utf8ToBytes(jsonConverter.toJson(barMsg));
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
