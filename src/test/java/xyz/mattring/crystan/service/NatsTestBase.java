package xyz.mattring.crystan.service;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

public class NatsTestBase {

    static Process natsBrokerProcess;

    @BeforeAll
    static void startLocalNatsBroker() {
        try {
            // local nats broker binary for windows is in ./src/test/resources/nats-server-v2.10.7/nats-server.exe
            natsBrokerProcess = new ProcessBuilder("./src/test/resources/nats-server-v2.10.7/nats-server.exe").start();
            System.out.println("Started natsBrokerProcess = " + natsBrokerProcess);
        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
    }

    @AfterAll
    static void stopLocalNatsBroker() {
        if (natsBrokerProcess != null) {
            natsBrokerProcess.destroy();
            System.out.println("Stopped natsBrokerProcess = " + natsBrokerProcess);
        }
    }
}
