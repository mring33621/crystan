package xyz.mattring.crystan.service;

public class TrackedMsg<T> {
    private final T msg;
    private String jobId;

    public TrackedMsg(T msg) {
        this.msg = msg;
    }

    public TrackedMsg(T msg, String jobId) {
        this.msg = msg;
        this.jobId = jobId;
    }

    public T getMsg() {
        return msg;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
}
