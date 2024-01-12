package xyz.mattring.crystan.service;

import xyz.mattring.crystan.util.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

class JobIdSerdeHelperTest {

    @org.junit.jupiter.api.Test
    void roundTripTestShortJobIdSmallPayload() {
        String jobId = "JobId-1";
        String testPayload = "Hello, world!";
        byte[] payload = testPayload.getBytes();
        byte[] payloadWithJobId = JobIdSerdeHelper.prependPayloadWithJobId(jobId, payload);
        Tuple2<String, byte[]> extracted = JobIdSerdeHelper.splitJobIdAndPayload(payloadWithJobId);
        assertEquals(jobId, extracted._1());
        assertEquals(testPayload, new String(extracted._2()));

        // add a test for findJobId here, because i'm lazy
        assertEquals(jobId, JobIdSerdeHelper.findJobId(payloadWithJobId));
    }

    @org.junit.jupiter.api.Test
    void roundTripTestLongJobIdBiggerPayload() {
        String jobId = "HelloI'mabigJobIdPrefix-20240112-1234567890";
        String testPayload = "This code uses ByteBuffer for efficient byte array manipulation, " +
                "checks the length of jobId, and explicitly uses UTF-8 encoding for string conversion. " +
                "Error handling for specific situations (like invalid inputs) should be added based on your application's requirements.";
        byte[] payload = testPayload.getBytes();
        byte[] payloadWithJobId = JobIdSerdeHelper.prependPayloadWithJobId(jobId, payload);
        Tuple2<String, byte[]> extracted = JobIdSerdeHelper.splitJobIdAndPayload(payloadWithJobId);
        assertEquals(jobId, extracted._1());
        assertEquals(testPayload, new String(extracted._2()));

        // add a test for findJobId here, because i'm lazy
        assertEquals(jobId, JobIdSerdeHelper.findJobId(payloadWithJobId));
    }

}