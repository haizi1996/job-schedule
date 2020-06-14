package com.hailin.job.schedule.core.job.trigger;

import com.hailin.job.schedule.core.basic.AbstractElasticJob;
import com.hailin.job.schedule.core.job.loop.GroupHandlers;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 调度器
 * @author zhanghailin
 */
public class Schedule {


    private static final String SHRINE_QUARTZ_WORKER = "-shrineWorker";

    private final AbstractElasticJob job;
    private Trigger trigger;

    private ScheduleWorker scheduleWorker;

    public Schedule(AbstractElasticJob job , final Trigger trigger) {
        this.job = job;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void shutdown() {
        scheduleWorker.halt();
    }
    public void start(){
        scheduleWorker = new ScheduleWorker(job , trigger.createTriggered(false , null) , trigger.createQuartzTrigger());
        if(trigger.isInitialTriggered()){
            trigger(null);
        }
        GroupHandlers.registerScheduleWorker(scheduleWorker);
    }

    public void awaitTermination(long timeout) {
        scheduleWorker.isShutDown();
    }
    public boolean isTermibated(){
        return scheduleWorker.isShutDown();
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
