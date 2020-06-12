package com.hailin.job.schedule.core.basic.java;

import com.hailin.shrine.job.ScheduleJobReturn;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

@Getter
@Setter
public class ShardingItemFutureTask implements Callable<ScheduleJobReturn> {

    private static Logger log = LoggerFactory.getLogger(ShardingItemFutureTask.class);

    private JavaShardingItemCallable callable;

    private Callable<?> doneFinallyCallback;

    private ScheduledFuture<?> timeoutFuture;

    private Future<?> callFuture;

    private boolean done = false;

    public ShardingItemFutureTask(JavaShardingItemCallable callable, Callable<?> doneFinallyCallback) {
        this.callable = callable;
        this.doneFinallyCallback = doneFinallyCallback;
    }

    public void reset() {
        done = false;
        callable.reset();
    }

    @Override
    public ScheduleJobReturn call() throws Exception {
        return null;
    }
}
