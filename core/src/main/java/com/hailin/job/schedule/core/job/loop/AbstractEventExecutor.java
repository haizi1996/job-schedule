package com.hailin.job.schedule.core.job.loop;

public abstract class AbstractEventExecutor implements EventExecutor {

    @Override
    public boolean inEventLoop() {
        return inEventLoop(Thread.currentThread());
    }

    @Override
    public EventExecutor next() {
        return this;
    }
}
