package xyz.mattring.crystan.service;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;
import xyz.mattring.crystan.util.Tuple2;

import java.util.function.Function;

/**
 * A base class for a Crystan server.
 * <p>
 * A Crystan server is a component that subscribes to a NATS subject, receives request messages,
 * performs some business logic, and publishes a response message.
 * <p>
 * The server core is implemented using a disruptor, which allows for asynchronous processing
 * of requests and responses.
 * <p>
 * String jobIds are propagated from request to response to help the client correlate requests with responses.
 *
 * @param <T> the type of the request message
 * @param <U> the type of the response message
 */
public abstract class ServerCore<T, U> implements BusConnector, Subscriber<TrackedMsg<T>>, Publisher<TrackedMsg<U>>, Runnable {

    static class ReqRespEvent<T, U> {
        T req;
        U resp;
        String jobId;

        void clear() {
            req = null;
            resp = null;
            jobId = null;
        }
    }

    final String subscribeSubject;
    final String publishSubject;
    final Function<byte[], T> reqMsgDeserializer;
    final Function<U, byte[]> respMsgSerializer;
    final Disruptor<ReqRespEvent<T, U>> disruptor;
    boolean running = false;

    public ServerCore(String subscribeSubject, String publishSubject, Function<byte[], T> reqMsgDeserializer, Function<U, byte[]> respMsgSerializer) {
        this.subscribeSubject = subscribeSubject;
        this.publishSubject = publishSubject;
        this.reqMsgDeserializer = reqMsgDeserializer;
        this.respMsgSerializer = respMsgSerializer;
        final int ringSize = 1024; // TODO: make configurable (must be power of 2)
        disruptor = new Disruptor<>(ReqRespEvent::new, ringSize, DaemonThreadFactory.INSTANCE);
        disruptor.handleEventsWith(this::handleRequestEvent, this::handleResponseEvent).then((event, sequence, endOfBatch) -> event.clear());
    }

    @Override
    public void run() {
        Tuple2<Dispatcher, Subscription> subParts = null;
        try {
            running = true;
            subParts = subscribe(this::processTrackedRequestAsync, this::deserializeTrackedRequest, subscribeSubject, getConnection());
            while (running) {
                performSideWork();
            }
        } finally {
            running = false;
            if (subParts != null) {
                subParts._1().unsubscribe(subParts._2());
            }
            if (disruptor != null) {
                disruptor.shutdown();
            }
        }
    }

    public void stop() {
        running = false;
    }

    /**
     * Deserializes a request message from a byte array.
     *
     * @param trackedRequestMsgBytes the serialized request message
     * @return the deserialized request message
     */
    TrackedMsg<T> deserializeTrackedRequest(byte[] trackedRequestMsgBytes) {
        Tuple2<String, byte[]> jobIdAndPayload = JobIdSerdeHelper.splitJobIdAndPayload(trackedRequestMsgBytes);
        String jobId = jobIdAndPayload._1();
        T requestMsg = reqMsgDeserializer.apply(jobIdAndPayload._2());
        return new TrackedMsg<>(jobId, requestMsg);
    }

    /**
     * Publishes a request message to the internal disruptor.
     *
     * @param trackedRequestMsg the request message to publish
     */
    void processTrackedRequestAsync(TrackedMsg<T> trackedRequestMsg) {
        disruptor.publishEvent((event, sequence) -> {
            event.req = trackedRequestMsg.getMsg();
            event.jobId = trackedRequestMsg.getJobId();
        });
    }

    /**
     * Handles a request event from the internal disruptor.
     *
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
        processTrackedResponseAsync(new TrackedMsg<>(event.jobId, responseMsg));
    }

    /**
     * Publishes a response message to the internal disruptor.
     *
     * @param trackedResponseMsg the response message to publish
     */
    void processTrackedResponseAsync(TrackedMsg<U> trackedResponseMsg) {
        disruptor.publishEvent((event, sequence) -> {
            event.resp = trackedResponseMsg.getMsg();
            event.jobId = trackedResponseMsg.getJobId();
        });
    }

    /**
     * Handles a response event from the internal disruptor.
     *
     * @param event
     * @param sequence
     * @param endOfBatch
     */
    void handleResponseEvent(ReqRespEvent<T, U> event, long sequence, boolean endOfBatch) {
        U responseMsg = event.resp;
        if (responseMsg == null) {
            return;
        }
        publishResponse(new TrackedMsg<>(event.jobId, responseMsg));
    }

    /**
     * Do your business work here:
     * Query a database, call a web service, etc.
     *
     * @param requestMsg the request message to prepare a response for
     * @return the response message
     */
    abstract U prepareResponse(T requestMsg); // TODO: make a pluggable Function<T, U> instead?

    /**
     * Serializes a response message to a byte array.
     *
     * @param trackedResponseMsg the response message to serialize
     * @return the serialized response message
     */
    byte[] serializeTrackedResponse(TrackedMsg<U> trackedResponseMsg) {
        return JobIdSerdeHelper.prependPayloadWithJobId(trackedResponseMsg.getJobId(), respMsgSerializer.apply(trackedResponseMsg.getMsg()));
    }

    /**
     * Publishes a response message to the NATS publishSubject.
     *
     * @param trackedResponseMsg the response message to publish
     */
    void publishResponse(TrackedMsg<U> trackedResponseMsg) {
        publish(trackedResponseMsg, this::serializeTrackedResponse, publishSubject, getConnection());
    }

    /**
     * Override this method to perform side work in the main run() loop.
     * By default, this method does nothing.
     */
    void performSideWork() {
        // use you imagination...
        // TODO: make this a pluggable Runnable instead?
    }
}
