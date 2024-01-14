package xyz.mattring.crystan.service;

public class TrackedMsg<T> {

    private final String jobId;
    private final T msg;

    public TrackedMsg(String jobId, T msg) {
        this.jobId = jobId;
        this.msg = msg;
    }

    public String getJobId() {
        return jobId;
    }

    public T getMsg() {
        return msg;
    }

}
