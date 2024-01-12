package xyz.mattring.crystan.service;

import xyz.mattring.crystan.util.Tuple2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class JobIdSerdeHelper {

    private static final int JOB_ID_SIZE = 50;

    /**
     * prepend payload with jobId
     *
     * @param jobId
     * @param payload
     * @return byte array containing a Job ID followed by a payload
     */
    public static byte[] prependPayloadWithJobId(String jobId, byte[] payload) {
        // Verify jobId length
        if (jobId.getBytes(StandardCharsets.UTF_8).length > JOB_ID_SIZE) {
            throw new IllegalArgumentException("Job ID exceeds maximum length of " + JOB_ID_SIZE + " bytes");
        }

        ByteBuffer buffer = ByteBuffer.allocate(JOB_ID_SIZE + payload.length);
        buffer.put(jobId.getBytes(StandardCharsets.UTF_8));
        // Advance position to compensate for any unfilled jobId space
        buffer.position(JOB_ID_SIZE);
        buffer.put(payload);
        return buffer.array();
    }

    /**
     * fast extraction of Job ID and payload from payloadWithJobId
     *
     * @param payloadWithJobId
     * @return tuple of Job ID and payload
     */
    public static Tuple2<String, byte[]> splitJobIdAndPayload(byte[] payloadWithJobId) {
        ByteBuffer buffer = ByteBuffer.wrap(payloadWithJobId);
        byte[] jobIdBytes = new byte[JOB_ID_SIZE];
        buffer.get(jobIdBytes, 0, JOB_ID_SIZE);
        String jobId = new String(jobIdBytes, StandardCharsets.UTF_8).trim();

        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        return new Tuple2<>(jobId, payload);
    }

    /**
     * fast extraction of Job ID from payloadWithJobId
     *
     * @param payloadWithJobId byte array containing a Job ID followed by a payload
     * @return null if payloadWithJobId is null or too short to contain a Job ID
     */
    public static String findJobId(byte[] payloadWithJobId) {
        if (payloadWithJobId == null || payloadWithJobId.length < JOB_ID_SIZE) {
            return null;
        }
        // Extract only the Job ID part
        byte[] jobIdBytes = new byte[JOB_ID_SIZE];
        System.arraycopy(payloadWithJobId, 0, jobIdBytes, 0, JOB_ID_SIZE);
        return new String(jobIdBytes, StandardCharsets.UTF_8).trim();
    }
}
