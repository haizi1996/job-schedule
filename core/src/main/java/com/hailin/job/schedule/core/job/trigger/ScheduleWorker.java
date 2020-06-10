package com.hailin.job.schedule.core.job.trigger;

import com.hailin.job.schedule.core.basic.AbstractElasticJob;
import com.hailin.shrine.job.common.exception.JobException;
import com.hailin.shrine.job.common.util.LogEvents;
import lombok.Getter;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.quartz.Trigger;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shrine的工作实体
 * @author zhanghailin
 */
public class ScheduleWorker implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleWorker.class);

    private final Object lock = new Object();

    private final AbstractElasticJob job ;

    private final Triggered notTriggered;

    @Getter
    private volatile OperableTrigger operableTrigger;

    @Getter
    private volatile boolean paused = false;

    @Getter
    private volatile Triggered triggered;
    //是否暂停
    private volatile boolean isPaused;

    @Getter
    private AtomicBoolean halted = new AtomicBoolean(false);

    public ScheduleWorker(AbstractElasticJob job, Triggered notTriggered, Trigger trigger) {
        this.job = job;
        this.notTriggered = notTriggered;
        this.triggered = notTriggered;
        initTrigger(trigger);
    }

    void reInitTrigger(Trigger trigger) {
        initTrigger(trigger);
        synchronized (lock){
            lock.notifyAll();
        }
    }
    private void initTrigger(Trigger trigger) {
        if (trigger == null){
            return;
        }
        if (!(trigger instanceof OperableTrigger )){
            throw new JobException("the trigger should be the instance of OperableTrigger");
        }
        this.operableTrigger = (OperableTrigger) trigger;
        Date date = this.operableTrigger.computeFirstFireTime(null);
        if(date == null){
            LOGGER.warn(LogEvents.ExecutorEvent.COMMON,
                    "Based on configured schedule, the given trigger {} will never fire.", trigger.getKey(),
                    job.getJobName());
        }
    }

    boolean isShutDown(){
        return halted.get();
    }


    @Override
    public void run() {

    }

    /**
     * 计算距离下次需要执行的毫秒差
     * @return -1，代表没有下次执行
     */
    Long calculateNextTime() {
        if (operableTrigger != null) {
            return -1L;
        }
        operableTrigger.updateAfterMisfire(null);
        long now = System.currentTimeMillis();
        Date nextFireTime = operableTrigger.getNextFireTime();
        if (nextFireTime != null) {
            return nextFireTime.getTime() - now;
        } else {
            return -1L;
        }
    }

    void halt(){
        synchronized (lock){
            halted.set(true);
            lock.notifyAll();
        }
    }
    void trigger(Triggered triggered){
        synchronized (lock){
            this.triggered = triggered == null ? notTriggered : triggered;
            lock.notifyAll();
        }
    }

    public Date getNextFireTimePausePeriodEffected(){
        if (operableTrigger == null){
            return null;
        }
        operableTrigger.updateAfterMisfire(null);
        Date nextFireTime = operableTrigger.getNextFireTime();
        while(nextFireTime != null && job.getConfigurationService().isInPausePeriod(nextFireTime)){
            nextFireTime = operableTrigger.getFireTimeAfter(nextFireTime);
        }
        return nextFireTime;
    }

    /**
     *
     */
    public void execute(){

        job.execute();
    }
}
