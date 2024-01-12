package xyz.mattring.crystan.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class JobIdGenerator {
    private final String prefix;
    private int todayYYYYMMDD;
    private long todayJobCount;

    public JobIdGenerator(String prefix) {
        final boolean validPrefix = isValidPrefix(prefix);
        if (!validPrefix) {
            throw new IllegalArgumentException("prefix must be non-null, non-empty, and contain only ASCII characters");
        }
        this.prefix = prefix;
        this.todayYYYYMMDD = calcTodayYYYYMMDD();
        todayJobCount = 1L;
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(
                this::checkIfNewDay, 2, 2, java.util.concurrent.TimeUnit.MINUTES);
        Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));
    }

    static boolean isValidPrefix(String prefix) {
        return prefix != null
                && !prefix.trim().isEmpty()
                && prefix.chars().allMatch(c -> c < 128);
    }

    private int calcTodayYYYYMMDD() {
        return Integer.parseInt(
                java.time.LocalDate.now().format(
                        java.time.format.DateTimeFormatter.BASIC_ISO_DATE));
    }

    private void checkIfNewDay() {
        final int today = calcTodayYYYYMMDD();
        synchronized (prefix) {
            if (today != todayYYYYMMDD) {
                todayYYYYMMDD = today;
                todayJobCount = 1L;
            }
        }
    }

    public String nextJobId() {
        synchronized (prefix) {
            return prefix + "-" + todayYYYYMMDD + "-" + todayJobCount++;
        }
    }

}
