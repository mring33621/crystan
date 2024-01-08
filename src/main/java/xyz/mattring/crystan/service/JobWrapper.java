package xyz.mattring.crystan.service;

import java.util.function.Supplier;

public class JobWrapper<T> {
    private final String jobId;
    private final T msg;

    public JobWrapper(Supplier<String> jobIdSupplier, T msg) {
        this(jobIdSupplier.get(), msg);
    }

    public JobWrapper(String jobId, T msg) {
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
