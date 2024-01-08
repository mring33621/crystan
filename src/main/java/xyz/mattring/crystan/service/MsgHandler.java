package xyz.mattring.crystan.service;

import io.nats.client.Subscription;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;

/**
 * This is a base class for a 'server' that receives requests, does some work, and then sends a response.
 *
 * @param <T>
 * @param <U>
 */
public abstract class MsgHandler<T, U> implements BusConnector, Subscriber<T>, Publisher<U>, Runnable {

    final String subscribeSubject;
    final String publishSubject;
    boolean running = false;

    public MsgHandler(String subscribeSubject, String publishSubject) {
        this.subscribeSubject = subscribeSubject;
        this.publishSubject = publishSubject;
    }

    @Override
    public void run() {
        running = true;
        final Subscription sub =
                subscribe(this::handleRequest, this::transformRequest, subscribeSubject, getConnection());
        while (running) {
            performMainThreadWork();
        }
        sub.unsubscribe();
    }

    abstract T transformRequest(byte[] msg);

    void handleRequest(T msg) {
        U response = prepareResponse(msg);
        publish(response, this::transformResponse, publishSubject, getConnection());
    }

    abstract U prepareResponse(T msg);

    abstract byte[] transformResponse(U msg);

    abstract void performMainThreadWork();

    public void stop() {
        running = false;
    }
}
