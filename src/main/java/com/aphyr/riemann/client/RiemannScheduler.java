package com.aphyr.riemann.client;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Supports periodic reporting of events.
public class RiemannScheduler {
    public static abstract class Task {
        public abstract void run(IRiemannClient r);
    }

    public final ScheduledThreadPoolExecutor pool;
    public final IRiemannClient client;

    public RiemannScheduler(final IRiemannClient client) {
        this(client, 1);
    }

    // Finer control over threadpool
    public RiemannScheduler(final IRiemannClient client, int poolSize) {
        this.client = client;
        pool = new ScheduledThreadPoolExecutor(poolSize);
        pool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public void shutdown() {
        pool.shutdown();
    }

    // Converts a callback to a runnable by closing over this pool's client
    protected Runnable runnableCallback(final Task c) {
        return new Runnable() {
            @Override
            public void run() {
                c.run(client);
            }
        };
    }

    // Schedule an arbitrary runnable to be run periodically; useful when
    // you already have a client handy. Interval is in ms.
    public ScheduledFuture every(long interval, Runnable f) {
        return every(interval, 0, f);
    }

    public ScheduledFuture every(long interval, Task c) {
        return every(interval, runnableCallback(c));
    }

    // Schedule an arbitrary runnable to be run periodically. Adjustable
    // units.
    public ScheduledFuture every(long interval, TimeUnit unit, Runnable f) {
        return every(interval, 0, unit, f);
    }

    public ScheduledFuture every(long interval, TimeUnit unit, Task c) {
        return every(interval, unit, runnableCallback(c));
    }

    // Schedule an arbitrary runnable to be run periodically, with initial
    // delay. Times in ms.
    public ScheduledFuture every(long interval, long delay, Runnable f) {
        return every(interval, delay, TimeUnit.MILLISECONDS, f);
    }

    public ScheduledFuture every(long interval, long delay, Task c) {
        return every(interval, delay, runnableCallback(c));
    }

    // Schedule an arbitrary runnable to be run periodically, with initial
    // delay and adjustable units.
    public ScheduledFuture every(long interval, long delay, TimeUnit unit, Runnable f) {
        return pool.scheduleAtFixedRate(f, delay, interval, unit);
    }

    public ScheduledFuture every(long interval, long delay, TimeUnit unit, Task c) {
        return every(interval, delay, unit, runnableCallback(c));
    }
}
