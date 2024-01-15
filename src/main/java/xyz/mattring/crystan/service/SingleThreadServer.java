package xyz.mattring.crystan.service;

import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import xyz.mattring.crystan.json.JsonConverter;
import xyz.mattring.crystan.msgbus.BusConnector;
import xyz.mattring.crystan.msgbus.Publisher;
import xyz.mattring.crystan.msgbus.Subscriber;
import xyz.mattring.crystan.util.BytesConverter;
import xyz.mattring.crystan.util.Tuple2;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * Receives requests and sends responses.
 * Assumes JSON message format with prepended jobId bytes.
 *
 * @param <A> reqType
 * @param <B> respType
 */
public class SingleThreadServer<A, B> implements BusConnector, Subscriber<TrackedMsg<A>>, Publisher<TrackedMsg<B>>, Runnable {

    private final String subjectA;
    private final String subjectB;
    private boolean running = false;
    final Queue<TrackedMsg<A>> reqQueue;
    final JsonConverter jsonConverter;
    final Class<A> reqType;
    final Function<A, B> reqHandler;

    public SingleThreadServer(String subjectA, String subjectB, Class<A> reqType, Function<A, B> reqHandler) {
        this.subjectA = subjectA;
        this.subjectB = subjectB;
        this.reqQueue = new ConcurrentLinkedQueue<>();
        this.jsonConverter = new JsonConverter() {
        };
        this.reqType = reqType;
        this.reqHandler = reqHandler;
    }

    void enqueueRequest(TrackedMsg<A> req) {
        reqQueue.add(req);
    }

    @Override
    public void run() {
        running = true;
        Tuple2<Dispatcher, Subscription> subParts = subscribe(
                this::enqueueRequest,
                this::deserializeRequest,
                subjectA,
                getConnection());
        while (running) {
            TrackedMsg<A> req = reqQueue.poll();
            if (req != null) {
                TrackedMsg<B> resp = processRequest(req);
                sendResponse(resp);
            }
        }
        subParts._1().unsubscribe(subParts._2());
    }

    public void stop() {
        running = false;
    }

    TrackedMsg<A> deserializeRequest(byte[] reqBytes) {
        final Tuple2<String, byte[]> jobIdAndPayload = JobIdSerdeHelper.splitJobIdAndPayload(reqBytes);
        final A req = jsonConverter.fromJson(BytesConverter.bytesToUtf8(jobIdAndPayload._2()), reqType);
        return new TrackedMsg<>(jobIdAndPayload._1(), req);
    }

    TrackedMsg<B> processRequest(TrackedMsg<A> req) {
        final String jobId = req.getJobId();
        final A reqMsg = req.getMsg();
        final B respMsg = reqHandler.apply(reqMsg);
        return new TrackedMsg<>(jobId, respMsg);
    }

    byte[] serializeResponse(TrackedMsg<B> resp) {
        final String jobId = resp.getJobId();
        final String msgJson = jsonConverter.toJson(resp.getMsg());
        return JobIdSerdeHelper.prependPayloadWithJobId(jobId, BytesConverter.utf8ToBytes(msgJson));
    }

    void sendResponse(TrackedMsg<B> resp) {
        publish(resp, this::serializeResponse, subjectB, getConnection());
    }

}




