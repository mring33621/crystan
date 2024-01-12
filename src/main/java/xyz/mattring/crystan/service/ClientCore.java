package xyz.mattring.crystan.service;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.nats.client.Subscription;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;

/**
 * A base class for a Crystan client.
 * <p>
 * A Crystan client is a component that publishes a request messages to a NATS subject and
 * receives a response messages on a different NATS subject.
 * <p>
 * The client core is implemented using a disruptor, which allows for asynchronous processing
 * of requests and responses.
 * <p>
 * I recommend that both the request and response messages implement the TrackedJob interface,
 * which allows for correlation of requests and responses. But do what you want.
 *
 * @param <T> the type of the request message
 * @param <U> the type of the response message
 */
public abstract class ClientCore<T, U> implements BusConnector, Publisher<T>, Subscriber<U>, Runnable {

    static class ReqRespEvent<T, U> {
        T req;
        U resp;

        void clear() {
            req = null;
            resp = null;
        }
    }

    final String publishSubject;
    final String subscribeSubject;
    boolean running = false;
    final Disruptor<ReqRespEvent<T, U>> disruptor;

    public ClientCore(String publishSubject, String subscribeSubject) {
        this.publishSubject = publishSubject;
        this.subscribeSubject = subscribeSubject;
        final int ringSize = 1024; // TODO: make configurable (must be power of 2)
        disruptor = new Disruptor<>(ReqRespEvent::new, ringSize, DaemonThreadFactory.INSTANCE);
        disruptor.handleEventsWith(this::handleResponseEvent, this::handleRequestEvent).then((event, sequence, endOfBatch) -> event.clear());
    }

    public U sendRequest(T req) {
        return sendRequestAndWaitForResponse(req);
    }

}
