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
 * @param <A> the type of the request message
 * @param <B> the type of the response message
 */
public class ServerCore<A, B> implements BusConnector, Subscriber<TrackedMsg<A>>, Publisher<TrackedMsg<B>>, Runnable {

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

    final String rcvReqSubject;
    final String sendRespSubject;
    final Function<byte[], A> reqMsgDeserializer;
    final Function<A, B> businessLogic;
    final Function<B, byte[]> respMsgSerializer;
    final Disruptor<ReqRespEvent<A, B>> disruptor;
    boolean running = false;

    public ServerCore(String rcvReqSubject, String sendRespSubject, Function<byte[], A> reqMsgDeserializer, Function<A, B> businessLogic, Function<B, byte[]> respMsgSerializer) {
        this.rcvReqSubject = rcvReqSubject;
        this.sendRespSubject = sendRespSubject;
        this.reqMsgDeserializer = reqMsgDeserializer;
        this.businessLogic = businessLogic;
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
            disruptor.start();
            subParts = subscribe(this::processTrackedRequestAsync, this::deserializeTrackedRequest, rcvReqSubject, getConnection());
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
    TrackedMsg<A> deserializeTrackedRequest(byte[] trackedRequestMsgBytes) {
        Tuple2<String, byte[]> jobIdAndPayload = JobIdSerdeHelper.splitJobIdAndPayload(trackedRequestMsgBytes);
        String jobId = jobIdAndPayload._1();
        A requestMsg = reqMsgDeserializer.apply(jobIdAndPayload._2());
        return new TrackedMsg<>(jobId, requestMsg);
    }

    /**
     * Publishes a request message to the internal disruptor.
     *
     * @param trackedRequestMsg the request message to publish
     */
    void processTrackedRequestAsync(TrackedMsg<A> trackedRequestMsg) {
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
    void handleRequestEvent(ReqRespEvent<A, B> event, long sequence, boolean endOfBatch) {
        A requestMsg = event.req;
        if (requestMsg == null) {
            return;
        }
        B responseMsg = prepareResponse(requestMsg);
        processTrackedResponseAsync(new TrackedMsg<>(event.jobId, responseMsg));
    }

    /**
     * Publishes a response message to the internal disruptor.
     *
     * @param trackedResponseMsg the response message to publish
     */
    void processTrackedResponseAsync(TrackedMsg<B> trackedResponseMsg) {
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
    void handleResponseEvent(ReqRespEvent<A, B> event, long sequence, boolean endOfBatch) {
        B responseMsg = event.resp;
        if (responseMsg == null) {
            return;
        }
        publishResponse(new TrackedMsg<>(event.jobId, responseMsg));
    }

    /**
     * Runs the business logic to prepare a response message.
     * @param requestMsg
     * @return responseMsg
     */
    B prepareResponse(A requestMsg) {
        return businessLogic.apply(requestMsg);
    }

    /**
     * Serializes a response message to a byte array.
     *
     * @param trackedResponseMsg the response message to serialize
     * @return the serialized response message
     */
    byte[] serializeTrackedResponse(TrackedMsg<B> trackedResponseMsg) {
        return JobIdSerdeHelper.prependPayloadWithJobId(trackedResponseMsg.getJobId(), respMsgSerializer.apply(trackedResponseMsg.getMsg()));
    }

    /**
     * Publishes a response message to the NATS publishSubject.
     *
     * @param trackedResponseMsg the response message to publish
     */
    void publishResponse(TrackedMsg<B> trackedResponseMsg) {
        publish(trackedResponseMsg, this::serializeTrackedResponse, sendRespSubject, getConnection());
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
