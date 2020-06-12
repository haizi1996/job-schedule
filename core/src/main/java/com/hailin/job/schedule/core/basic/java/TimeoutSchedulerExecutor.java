package com.hailin.job.schedule.core.basic.java;

import com.hailin.job.schedule.core.basic.threads.ScheduleThreadFactory;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TimeoutSchedulerExecutor {

    private static Logger log = LoggerFactory.getLogger(TimeoutSchedulerExecutor.class);

    private static ConcurrentHashMap<String, ScheduledThreadPoolExecutor> scheduledThreadPoolExecutorMap = new ConcurrentHashMap<>();

    private TimeoutSchedulerExecutor() {

    }
    public static synchronized ScheduledThreadPoolExecutor createScheduler(String executorName) {
        if (!scheduledThreadPoolExecutorMap.containsKey(executorName)) {
            ScheduledThreadPoolExecutor timeoutExecutor = new ScheduledThreadPoolExecutor(
                    Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                    new ScheduleThreadFactory(executorName + "-timeout-watchdog", false));
            timeoutExecutor.setRemoveOnCancelPolicy(true);
            scheduledThreadPoolExecutorMap.put(executorName, timeoutExecutor);
            return timeoutExecutor;
        }
        return scheduledThreadPoolExecutorMap.get(executorName);
    }

    private static ScheduledThreadPoolExecutor getScheduler(String executorName) {
        return scheduledThreadPoolExecutorMap.get(executorName);
    }

    public static final void shutdownScheduler(String executorName) {
        if (getScheduler(executorName) != null) {
            getScheduler(executorName).shutdown();
            scheduledThreadPoolExecutorMap.remove(executorName);
        }
    }
    public static final void scheduleTimeoutJob(String executorName, int timeoutSeconds,
                                                ShardingItemFutureTask shardingItemFutureTask) {
        ScheduledFuture<?> timeoutFuture = getScheduler(executorName)
                .schedule(new TimeoutHandleTask(shardingItemFutureTask), timeoutSeconds, TimeUnit.SECONDS);
        shardingItemFutureTask.setTimeoutFuture(timeoutFuture);
    }

    @AllArgsConstructor
    private static class TimeoutHandleTask implements Runnable{

        private ShardingItemFutureTask shardingItemFutureTask;

        @Override
        public void run() {

            try {
                JavaShardingItemCallable javaShardingItemCallable = shardingItemFutureTask.getCallable();
                if (!shardingItemFutureTask.isDone() && javaShardingItemCallable.setTimeout()){
                    String jobName = javaShardingItemCallable.getJobName();
                    Integer item = javaShardingItemCallable.getItem();
                    log.info( "Force stop timeout job, jobName:{}, item:{}", jobName, item);
                    // 调用beforeTimeout函数
                    javaShardingItemCallable.beforeTimeout();
                    // 强杀
                    ShardingItemFutureTask.killRunningBusinessThread(shardingItemFutureTask);
                }
            }catch (Throwable t) {
                JavaShardingItemCallable javaShardingItemCallable = shardingItemFutureTask.getCallable();
                if (javaShardingItemCallable != null) {
                    log.warn( javaShardingItemCallable.getJobName(), "Failed to force stop timeout job", t);
                } else {
                    log.warn( "unknown job", "Failed to force stop timeout job", t);
                }
            }

        }
    }

}
