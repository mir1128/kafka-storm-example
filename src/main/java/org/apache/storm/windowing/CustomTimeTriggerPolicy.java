package org.apache.storm.windowing;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.topology.FailedException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CustomTimeTriggerPolicy<T> implements TriggerPolicy<T> {
    private final TriggerHandler handler;
    private final EvictionPolicy<T> evictionPolicy;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> executorFuture;
    private long duration;

    public CustomTimeTriggerPolicy(long millis, TriggerHandler handler, EvictionPolicy<T> evictionPolicy) {
        this.handler = handler;
        this.evictionPolicy = evictionPolicy;
        this.duration = millis;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("custom-time-trigger-policy-%d")
                .setDaemon(true)
                .build();
        this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    @Override
    public void track(Event<T> event) {
        checkFailures();
    }

    @Override
    public void reset() {
        checkFailures();
    }

    @Override
    public void start() {
        executorFuture = executor.scheduleAtFixedRate(newTriggerTask(), duration, duration, TimeUnit.MILLISECONDS);
    }

    private void checkFailures() {
        if (executorFuture != null && executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException ex) {
                log.error("Got exception ", ex);
                throw new FailedException(ex);
            } catch (ExecutionException ex) {
                log.error("Got exception ", ex);
                throw new FailedException(ex.getCause());
            }
        }
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private Runnable newTriggerTask() {
        return () -> {
            if (evictionPolicy != null) {
                evictionPolicy.setContext(new DefaultEvictionContext(null, null, null, duration));
            }
            handler.onTrigger();
        };
    }

    @Override
    public String toString() {
        return "CustomTimeTriggerPolicy{" +
                "duration=" + duration +
                '}';
    }
}
