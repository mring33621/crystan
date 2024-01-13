package xyz.mattring.crystan.service;

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

public class ClientCore<T, U> implements BusConnector, Publisher<TrackedMsg<T>>, Subscriber<TrackedMsg<U>>, Runnable {

    final String publishSubject;
    final String subscribeSubject;
    final Function<T, byte[]> reqMsgSerializer;
    final Function<byte[], U> respMsgDeserializer;
    final Map<String, Consumer<U>> respHandlers;
    final JobIdGenerator jobIdGenerator;
    boolean running = false;

    public ClientCore(String publishSubject, String subscribeSubject, Function<T, byte[]> reqMsgSerializer, Function<byte[], U> respMsgDeserializer) {
        this.publishSubject = publishSubject;
        this.subscribeSubject = subscribeSubject;
        this.reqMsgSerializer = reqMsgSerializer;
        this.respMsgDeserializer = respMsgDeserializer;
        this.respHandlers = new ConcurrentHashMap<>(); // TODO: expire old handlers that didn't get a response
        // TODO: make clientId configurable
        final String clientId =
                this.getClass().getSimpleName() + "-" + ThreadLocalRandom.current().nextInt(1000);
        this.jobIdGenerator = new JobIdGenerator(clientId);
    }

    @Override
    public void run() {
        Subscription sub = null;
        try {
            running = true;
            sub = subscribe(
                    this::processTrackedResponse,
                    this::deserializeTrackedResponse,
                    subscribeSubject,
                    getConnection());
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

    public void stop() {
        running = false;
    }

    TrackedMsg<U> deserializeTrackedResponse(byte[] trackedResponseMsgBytes) {
        TrackedMsg<U> trackedResponseMsg = null;
        final String jobId = JobIdSerdeHelper.findJobId(trackedResponseMsgBytes);
        if (jobId != null && respHandlers.containsKey(jobId)) {
            // TODO: consider optimizing this by not deserializing the response if there is no handler for the jobId
            // TODO: consider optimizing this by not extracting the jobId twice
            final Tuple2<String, byte[]> jobIdAndPayload = JobIdSerdeHelper.splitJobIdAndPayload(trackedResponseMsgBytes);
            final U requestMsg = respMsgDeserializer.apply(jobIdAndPayload._2());
            trackedResponseMsg = new TrackedMsg<>(requestMsg, jobId);
        }
        return trackedResponseMsg;
    }

    void processTrackedResponse(TrackedMsg<U> trackedResponseMsg) {
        if (trackedResponseMsg != null) {
            final String jobId = trackedResponseMsg.getJobId();
            final Consumer<U> respHandler = respHandlers.remove(jobId);
            if (respHandler != null) {
                respHandler.accept(trackedResponseMsg.getMsg());
            }
        }
    }

    public void sendRequest(T req, Consumer<U> respHandler) {
        final String jobId = jobIdGenerator.nextJobId();
        respHandlers.put(jobId, respHandler);
        final TrackedMsg<T> trackedRequestMsg = new TrackedMsg<>(req, jobId);
        publish(trackedRequestMsg, this::serializeTrackedRequest, publishSubject, getConnection());
    }

    byte[] serializeTrackedRequest(TrackedMsg<T> trackedRequestMsg) {
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
}
