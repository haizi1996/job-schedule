package com.hailin.job.schedule.core.job.loop;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface EventExecutorGroup extends Iterable<EventExecutor> {

    boolean isShuttingDown();

    EventExecutor next();

    @Override
    Iterator<EventExecutor> iterator();

    Future<?> submit(Runnable task);

    <T> Future<T> submit(Runnable task, T result);

    <T> Future<T> submit(Callable<T> task);

    boolean isTerminated();
}
