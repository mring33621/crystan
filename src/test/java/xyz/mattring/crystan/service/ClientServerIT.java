package xyz.mattring.crystan.service;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import xyz.mattring.crystan.json.JsonConverter;
import xyz.mattring.crystan.util.BytesConverter;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.function.Function;

public class ClientServerIT {

    private static Process natsBrokerProcess;

    @BeforeAll
    static void startLocalNatsBroker() {
        try {
            // local nats broker binary for windows is in ./src/test/resources/nats-server-v2.10.7/nats-server.exe
            natsBrokerProcess = new ProcessBuilder("./src/test/resources/nats-server-v2.10.7/nats-server.exe").start();
        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
    }

    static class Foo {
        final String val1;
        final Double val2;

        public Foo(String val1, Double val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        public String getVal1() {
            return val1;
        }

        public Double getVal2() {
            return val2;
        }

        @Override
        public String toString() {
            return "Foo{" +
                    "val1='" + val1 + '\'' +
                    ", val2=" + val2 +
                    '}';
        }
    }

    static class Bar extends Foo {
        final Integer val3;

        public Bar(String val1, Double val2, Integer val3) {
            super(val1, val2);
            this.val3 = val3;
        }

        public Integer getVal3() {
            return val3;
        }

        @Override
        public String toString() {
            return "Bar{" +
                    "val1='" + val1 + '\'' +
                    ", val2=" + val2 +
                    ", val3='" + val3 + '\'' +
                    '}';
        }
    }

    static class TestJsonConverter implements JsonConverter {
    }

    @Test
    public void testClientServer() {
        final List<String> clientTimeline = new Vector<>();
        final List<String> serverTimeline = new Vector<>();

        final TestJsonConverter jsonConverter = new TestJsonConverter();
        String subject1 = "it.test.subject1";
        String subject2 = "it.test.subject2";

        ClientCore<Foo, Bar> testClientCore = new ClientCore<>(
                subject2, // client sends requests here
                subject1, // client receives responses here
                foo -> { // foo serializer
                    return BytesConverter.utf8ToBytes(jsonConverter.toJson(foo));
                },
                bytes -> { // bar deserializer
                    return jsonConverter.fromJson(BytesConverter.bytesToUtf8(bytes), Bar.class);
                }
        );

        Function<byte[], Foo> fooDeserializer = bytes -> {
            return jsonConverter.fromJson(BytesConverter.bytesToUtf8(bytes), Foo.class);
        };

        Function<Bar, byte[]> barSerializer = bar -> {
            return BytesConverter.utf8ToBytes(jsonConverter.toJson(bar));
        };

        ServerCore<Foo, Bar> testServerCore = new ServerCore<>(
                subject1, // server receives requests here
                subject2, // server sends responses here
                fooDeserializer,
                barSerializer) {

            @Override
            Bar prepareResponse(Foo fooIn) {
                System.out.println("prepareResponse(" + fooIn + ")");
                serverTimeline.add(fooIn.toString());
                Bar barOut = new Bar(
                        fooIn.getVal1(),
                        fooIn.getVal2(),
                        (int) (fooIn.getVal2() + 1.0d));
                serverTimeline.add(barOut.toString());
                System.out.println("returning " + barOut);
                return barOut;
            }
        };

        SingleThreadServer<Foo, Bar> testSingleThreadServer = new SingleThreadServer<>(
                subject1, // server receives requests here
                subject2, // server sends responses here
                Foo.class,
                fooIn -> {
                    System.out.println("prepareResponse(" + fooIn + ")");
                    serverTimeline.add(fooIn.toString());
                    Bar barOut = new Bar(
                            fooIn.getVal1(),
                            fooIn.getVal2(),
                            (int) (fooIn.getVal2() + 1.0d));
                    serverTimeline.add(barOut.toString());
                    System.out.println("returning " + barOut);
                    return barOut;
                }
        );

        Thread serverThread = new Thread(testSingleThreadServer);
        serverThread.start();
        Thread clientThread = new Thread(testClientCore);
        clientThread.start();

        String[] testStrings = new String[]{
                "This is a test.",
                "This is only a test.",
                "This is a test of the emergency broadcast system.",
                "This is only a test.",
                "If this were a real emergency, you would be instructed to tune to your local emergency broadcast station.",
                "This concludes this test of the emergency broadcast system.",
                "This is only a test.",
                "This is a test.",
                "This is only a test.",
                "Hello, world!"
        };
        int i = 0;
        for (String testString : testStrings) {
            Foo foo = new Foo(testString, (double) i++);
            clientTimeline.add(foo.toString());
            testClientCore.sendRequest(foo, bar -> {
                clientTimeline.add(bar.toString());
            });
        }

        while (clientTimeline.size() < testStrings.length * 2) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException iex) {
                throw new RuntimeException(iex);
            }
        }

        testClientCore.stop();
//        testServerCore.stop();
        testSingleThreadServer.stop();

        System.out.println("clientTimeline:" + clientTimeline);
        System.out.println("serverTimeline:" + serverTimeline);
    }

    @AfterAll
    static void stopLocalNatsBroker() {
        if (natsBrokerProcess != null) {
            natsBrokerProcess.destroy();
        }
    }
}
