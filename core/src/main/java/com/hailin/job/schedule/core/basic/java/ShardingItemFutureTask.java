package com.hailin.job.schedule.core.basic.java;

import com.hailin.shrine.job.ScheduleJobReturn;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
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

    public static void killRunningBusinessThread(ShardingItemFutureTask shardingItemFutureTask) {
        JavaShardingItemCallable shardingItemCallable = shardingItemFutureTask.getCallable();
        Thread businessThread = shardingItemCallable.getCurrentThread();
        if (Objects.nonNull(businessThread)){
            try {
                if (!isBusinessBreak(shardingItemFutureTask , shardingItemCallable)){
                    log.info( "try to interrupt business thread");
                    businessThread.interrupt();
                    for (int i = 0; i < 20; i++) {
                        if (!isBusinessBreak(shardingItemFutureTask , shardingItemCallable)){
                            log.info("interrupt business thread done");
                            return;
                        }
                        Thread.sleep(100L);
                    }
                }
                // stop thread
                while (!isBusinessBreak(shardingItemFutureTask, shardingItemCallable)) {
                    log.info( "try to force stop business thread");
                    businessThread.stop();
                    if (isBusinessBreak(shardingItemFutureTask, shardingItemCallable)) {
                        log.info( "force stop business thread done");
                        return;
                    }
                    Thread.sleep(50L);
                }
                log.info( "kill business thread done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else {
            log.warn( "business thread is null while killing it");
        }
    }

    public void reset() {
        done = false;
        callable.reset();
    }

    @Override
    public ScheduleJobReturn call() throws Exception {
        Thread.currentThread().setUncaughtExceptionHandler((t,e)->{
            if (e instanceof IllegalMonitorStateException || e instanceof ThreadDeath) {
                log.warn("business thread pool maybe crashed", e);
                if (callFuture != null) {
                    callFuture.cancel(false);
                }
                log.warn("close the old business thread pool, and re-create new one");
                callable.getScheduleJob().getJobScheduler().reCreateExecutorService();
            }
        });
        try {
            ScheduleJobReturn ret = callable.call();
            return ret;
        }finally {
            done();
            log.debug( "job:[{}] item:[{}] finish execution, which takes {}ms",
                    callable.getJobName(), callable.getItem(), callable.getExecutionTime());
        }
    }

    private void done() {
        if (timeoutFuture != null){
            timeoutFuture.cancel(true);
            timeoutFuture = null;
        }
        if (done){
            return;
        }
        done = true;
        try {
            try {
                if (callable.isTimeout()){
                    callable.onTimeout();
                }
            }catch (Throwable t){
                log.error( t.toString(), t);
            }
            try {
                if (callable.isForceStop()) {
                    callable.postForceStop();
                }
            } catch (Throwable t) {
                log.error( t.toString(), t);
            }
            callable.checkAndSetSaturnJobReturn();

            callable.afterExecution();

        }finally {
            try {
                if (Objects.nonNull(doneFinallyCallback)){
                    doneFinallyCallback.call();
                }
            }catch (Exception e){
                log.error( e.toString(), e);
            }
        }
    }
    private static boolean isBusinessBreak(ShardingItemFutureTask shardingItemFutureTask,
                                           JavaShardingItemCallable shardingItemCallable) {
        return shardingItemCallable.isBreakForceStop() || shardingItemFutureTask.isDone();
    }
}
