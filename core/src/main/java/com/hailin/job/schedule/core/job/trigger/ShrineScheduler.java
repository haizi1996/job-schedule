package com.hailin.job.schedule.core.job.trigger;

import com.hailin.job.schedule.core.basic.AbstractElasticJob;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 调度器
 * @author zhanghailin
 */
public class ShrineScheduler {


    private static final String SHRINE_QUARTZ_WORKER = "-shrineWorker";

    private final AbstractElasticJob job;
    private Trigger trigger;
    private final ExecutorService executor ;

    private ScheduleWorker scheduleWorker;

    public ShrineScheduler(AbstractElasticJob job , final Trigger trigger) {
        this.job = job;
        this.executor = Executors.newSingleThreadExecutor((r)->{
            Thread t = new Thread(r,
                    job.getExecutorName() + "_" + job.getConfigurationService().getJobName() + SHRINE_QUARTZ_WORKER);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        });
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void shutdown() {
        scheduleWorker.halt();
        executor.shutdown();
    }
    public void start(){
        scheduleWorker = new ScheduleWorker(job , trigger.createTriggered(false , null) , trigger.createQuartzTrigger());
        if(trigger.isInitialTriggered()){
            trigger(null);
        }
        executor.submit(scheduleWorker);
    }

    public void awaitTermination(long timeout) {
        try {
            executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public boolean isTermibated(){
        return executor.isTerminated();
    }

    public void reInitializeTrigger() {
        scheduleWorker.reInitTrigger(trigger.createQuartzTrigger());
    }

    public Date getNextFireTimePausePeriodEffected() {
        return scheduleWorker.getNextFireTimePausePeriodEffected();
    }
    public void trigger(String triggeredDataStr) {
        scheduleWorker.trigger(trigger.createTriggered(true , triggeredDataStr));
    }

    public boolean isShutdown(){
        return scheduleWorker.isShutDown();
    }
}
