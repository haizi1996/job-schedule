package com.hailin.job.schedule.core.job.loop;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public abstract class AbstractEventExecutorGroup implements EventExecutorGroup {

    @Override
    public Future<?> submit(Runnable task) {
        return next().submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return next().submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return next().submit(task);
    }

    @Override
    public boolean isShuttingDown() {
        return false;
    }

}
