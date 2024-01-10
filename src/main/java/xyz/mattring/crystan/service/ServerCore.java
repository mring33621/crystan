package xyz.mattring.crystan.service;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.nats.client.Subscription;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;

/**
 * A base class for a Crystan server.
 * <p>
 * A Crystan server is a component that subscribes to a NATS subject, receives request messages,
 * performs some business logic, and publishes a response message.
 * <p>
 * The server core is implemented using a disruptor, which allows for asynchronous processing
 * of requests and responses.
 *
 * @param <T> the type of the request message
 * @param <U> the type of the response message
 */
public abstract class ServerCore<T, U> implements BusConnector, Subscriber<T>, Publisher<U>, Runnable {

    static class ReqRespEvent<T, U> {
        T req;
        U resp;

        void clear() {
            req = null;
            resp = null;
        }
    }

    final String subscribeSubject;
    final String publishSubject;
    boolean running = false;
    final Disruptor<ReqRespEvent<T, U>> disruptor;

    public ServerCore(String subscribeSubject, String publishSubject) {
        this.subscribeSubject = subscribeSubject;
        this.publishSubject = publishSubject;
        final int ringSize = 1024; // TODO: make configurable (must be power of 2)
        disruptor = new Disruptor<>(ReqRespEvent::new, ringSize, DaemonThreadFactory.INSTANCE);
        disruptor.handleEventsWith(this::handleRequestEvent, this::handleResponseEvent).then((event, sequence, endOfBatch) -> event.clear());
    }

    @Override
    public void run() {
        Subscription sub = null;
        try {
            running = true;
            sub = subscribe(this::processRequestAsync, this::deserializeRequest, subscribeSubject, getConnection());
            while (running) {
                performSideWork();
            }
        } finally {
            running = false;
            if (sub != null) {
                sub.unsubscribe();
            }
        }
    }

    /**
     * Deserializes a request message from a byte array.
     *
     * @param requestMsgBytes the byte array to deserialize
     * @return the deserialized request message
     */
    abstract T deserializeRequest(byte[] requestMsgBytes);

    /**
     * Do your business work here:
     * Query a database, call a web service, etc.
     *
     * @param requestMsg the request message to prepare a response for
     * @return the response message
     */
    abstract U prepareResponse(T requestMsg);

    /**
     * Serializes a response message to a byte array.
     *
     * @param responseMsg the response message to serialize
     * @return the serialized response message
     */
    abstract byte[] serializeResponse(U responseMsg);

    /**
     * Publishes a request message to the internal disruptor.
     *
     * @param requestMsg the request message to publish
     */
    void processRequestAsync(T requestMsg) {
        disruptor.publishEvent((event, sequence) -> event.req = requestMsg);
    }

    /**
     * Handles a request event from the internal disruptor.
     * @param event
     * @param sequence
     * @param endOfBatch
     */
    void handleRequestEvent(ReqRespEvent<T, U> event, long sequence, boolean endOfBatch) {
        T requestMsg = event.req;
        if (requestMsg == null) {
            return;
        }
        U responseMsg = prepareResponse(requestMsg);
        processResponseAsync(responseMsg);
    }

    /**
     * Publishes a response message to the internal disruptor.
     *
     * @param responseMsg the response message to publish
     */
    void processResponseAsync(U responseMsg) {
        disruptor.publishEvent((event, sequence) -> event.resp = responseMsg);
    }

    /**
     * Handles a response event from the internal disruptor.
     * @param event
     * @param sequence
     * @param endOfBatch
     */
    void handleResponseEvent(ReqRespEvent<T, U> event, long sequence, boolean endOfBatch) {
        U responseMsg = event.resp;
        if (responseMsg == null) {
            return;
        }
        publishResponse(responseMsg);
    }

    /**
     * Publishes a response message to the NATS publishSubject.
     *
     * @param responseMsg the response message to publish
     */
    void publishResponse(U responseMsg) {
        publish(responseMsg, this::serializeResponse, publishSubject, getConnection());
    }

    /**
     * Override this method to perform side work in the main run() loop.
     * By default, this method does nothing.
     */
    void performSideWork() {
        // use you imagination...
    }
}
