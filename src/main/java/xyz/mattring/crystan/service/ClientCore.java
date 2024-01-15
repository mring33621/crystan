package xyz.mattring.crystan.service;

import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;
import xyz.mattring.crystan.util.Tuple2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;

public class ClientCore<A, B> implements BusConnector, Publisher<TrackedMsg<A>>, Subscriber<TrackedMsg<B>>, Runnable {

    final String sendReqSubject;
    final String rcvRespSubject;
    final Function<A, byte[]> reqMsgSerializer;
    final Function<byte[], B> respMsgDeserializer;
    final Map<String, Consumer<B>> registeredRespHandlers;
    final JobIdGenerator jobIdGenerator;
    boolean running = false;

    public ClientCore(String sendReqSubject, String rcvRespSubject, Function<A, byte[]> reqMsgSerializer, Function<byte[], B> respMsgDeserializer) {
        this.sendReqSubject = sendReqSubject;
        this.rcvRespSubject = rcvRespSubject;
        this.reqMsgSerializer = reqMsgSerializer;
        this.respMsgDeserializer = respMsgDeserializer;
        this.registeredRespHandlers = new ConcurrentHashMap<>(); // TODO: expire handlers that never got a response?
        // TODO: make clientId configurable
        final String clientId =
                this.getClass().getSimpleName() + "-" + ThreadLocalRandom.current().nextInt(1000);
        this.jobIdGenerator = new JobIdGenerator(clientId);
    }

    @Override
    public void run() {
        Tuple2<Dispatcher, Subscription> subParts = null;
        try {
            running = true;
            subParts = subscribe(
                    this::processTrackedResponse,
                    this::deserializeTrackedResponse,
                    rcvRespSubject,
                    getConnection());
            while (running) {
                performSideWork();
            }
        } finally {
            running = false;
            if (subParts != null) {
                subParts._1().unsubscribe(subParts._2());
            }
        }
    }

    public void stop() {
        running = false;
    }

    TrackedMsg<B> deserializeTrackedResponse(byte[] trackedResponseMsgBytes) {
        TrackedMsg<B> trackedResponseMsg = null;
        final String jobId = JobIdSerdeHelper.findJobId(trackedResponseMsgBytes);
        if (jobId != null && registeredRespHandlers.containsKey(jobId)) {
            // TODO: consider optimizing this by not deserializing the response if there is no handler for the jobId
            // TODO: consider optimizing this by not extracting the jobId twice
            final Tuple2<String, byte[]> jobIdAndPayload = JobIdSerdeHelper.splitJobIdAndPayload(trackedResponseMsgBytes);
            final B requestMsg = respMsgDeserializer.apply(jobIdAndPayload._2());
            trackedResponseMsg = new TrackedMsg<>(jobId, requestMsg);
        }
        return trackedResponseMsg;
    }

    void processTrackedResponse(TrackedMsg<B> trackedResponseMsg) {
        if (trackedResponseMsg != null) {
            final String jobId = trackedResponseMsg.getJobId();
            final Consumer<B> respHandler = registeredRespHandlers.remove(jobId);
            if (respHandler != null) {
                respHandler.accept(trackedResponseMsg.getMsg());
            }
        }
    }

    public void sendRequest(A req, Consumer<B> respHandler) {
        final String jobId = jobIdGenerator.nextJobId();
        registeredRespHandlers.put(jobId, respHandler);
        final TrackedMsg<A> trackedRequestMsg = new TrackedMsg<>(jobId, req);
        publish(trackedRequestMsg, this::serializeTrackedRequest, sendReqSubject, getConnection());
    }

    byte[] serializeTrackedRequest(TrackedMsg<A> trackedRequestMsg) {
        return JobIdSerdeHelper.prependPayloadWithJobId(
                trackedRequestMsg.getJobId(), reqMsgSerializer.apply(trackedRequestMsg.getMsg()));
    }

    /**
     * Override this method to perform side work in the main run() loop.
     * By default, this method does nothing.
     */
    void performSideWork() {
        // use you imagination...
        // TODO: make this a pluggable Runnable instead?
    }

    /**
     * Returns the number of registered response handlers.
     * @return num int
     */
    public int getNumHandlers() {
        return registeredRespHandlers.size();
    }
}
