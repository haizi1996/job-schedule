package com.hailin.job.schedule.core.job.loop;

import com.hailin.job.schedule.core.job.trigger.ScheduleWorker;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface EventExecutor {

    EventExecutor next();

    boolean inEventLoop();

    boolean inEventLoop(Thread thread);

    boolean isShuttingDown();

    boolean isShutdown();

    boolean isTerminated();

    void submit(Runnable task);

    void shutdownGracefully();

    boolean awaitTermination(int maxValue, TimeUnit seconds) throws InterruptedException;

    /**
     * 注册一个调度节点
     */
    boolean registerScheduleWorker(ScheduleWorker saturnWorker);
}
