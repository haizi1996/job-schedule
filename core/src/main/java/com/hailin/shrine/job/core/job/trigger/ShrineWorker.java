package com.hailin.shrine.job.core.job.trigger;

import com.hailin.shrine.job.common.exception.JobException;
import com.hailin.shrine.job.common.util.LogEvents;
import com.hailin.shrine.job.core.basic.AbstractElasticJob;
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
public class ShrineWorker implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShrineWorker.class);

    private final Object lock = new Object();

    private final AbstractElasticJob job ;

    private final Triggered notTriggered;
    private volatile OperableTrigger operableTrigger;

    private volatile boolean paused = false;
    private volatile Triggered triggered;
    //是否暂停
    private volatile boolean isPaused;

    private AtomicBoolean halted = new AtomicBoolean(false);

    public ShrineWorker(AbstractElasticJob job, Triggered notTriggered, Trigger trigger) {
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

    void checkJobBefore(){
        synchronized (lock) {
            //如果任务暂停，循环执行等待1秒，直至任务不是暂停状态
            while (paused && !isShutDown()) {
                try {
                    lock.wait(1000L );
                } catch (InterruptedException e) {
                    LOGGER.error("", e);
                }
            }
        }
    }

    @Override
    public void run() {
        while (!isShutDown()){
            try{
                checkJobBefore();
                //如果任务终止 退出
                if (isShutDown()) {
                    break;
                }
                //该任务下次执行需要多长毫秒
                long timeUnitTrigger = calculateNextTime();
                //没有下次执行时间，初始化为false
                boolean noFireTime = timeUnitTrigger == 1L ? false : true;

                while (!noFireTime && timeUnitTrigger > 2){
                    synchronized (lock){
                        if (isShutDown()){
                            break;
                        }

                        //如果是立即执行
                        if (triggered.isYes()){
                            break;
                        }
                        lock.wait(timeUnitTrigger);
                        //该任务下次执行需要多长毫秒
                        timeUnitTrigger = calculateNextTime();
                        //没有下次执行时间，初始化为false
                        noFireTime = timeUnitTrigger == 1L ? false : true;
                    }
                }
                boolean execute ;
                 //当前执行数据
                Triggered currentTriggered = notTriggered;
                //触发条件只有两个条件:1. 执行时间到了，2 点击立即执行
                synchronized (lock){
                    execute = !isShutDown() && !paused;
                    //立即执行
                    if (triggered.isYes()){
                        //重置立即执行
                        currentTriggered = triggered;
                        triggered = notTriggered;
                    }else if (execute){
                        //非立即执行 ， 可能是执行时间到了，也可能是没有下次执行
                        execute = execute && ! noFireTime;
                        // 有下次执行时间
                        if (execute){
                            if (operableTrigger != null) {
                                operableTrigger.triggered(null);
                            }
                        }else {
                            lock.wait(1000L);
                        }
                    }
                }
                //到达执行条件，立即执行
                if (execute) {
                    job.execute(currentTriggered);
                }

            }catch (Exception e){
                LOGGER.error( job.getJobName(), e.getMessage(), e);
            }
        }
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

    Date getNextFireTimePausePeriodEffected(){
        if (operableTrigger == null){
            return null;
        }
        operableTrigger.updateAfterMisfire(null);
        Date nextFireTime = operableTrigger.getNextFireTime();
        while(nextFireTime != null && job.getConfigurationService().is){
            nextFireTime = operableTrigger.getFireTimeAfter(nextFireTime);
        }
        return nextFireTime;
    }
}
