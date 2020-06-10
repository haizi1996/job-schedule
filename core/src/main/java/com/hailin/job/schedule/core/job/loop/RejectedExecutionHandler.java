package com.hailin.job.schedule.core.job.loop;


public interface RejectedExecutionHandler {

    void rejected(Runnable task, SingleThreadEventExecutor executor);
}
