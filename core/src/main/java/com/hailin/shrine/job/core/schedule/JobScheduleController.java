package com.hailin.shrine.job.core.schedule;


import com.hailin.shrine.job.common.exception.JobSystemException;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

/**
 * 作业调度控制器
 * @author zhanghailin
 */
public class JobScheduleController {

    private final Scheduler scheduler;

    private final JobDetail jobDetail;

    private final String triggerIdentity;

    public JobScheduleController(Scheduler scheduler, JobDetail jobDetail, String triggerIdentity) {
        this.scheduler = scheduler;
        this.jobDetail = jobDetail;
        this.triggerIdentity = triggerIdentity;
    }

    /**
     * 调度作业.
     *
     * @param cron CRON表达式
     */
    public void scheduleJob(final String cron) {

        try{
            if(!scheduler.checkExists(jobDetail.getKey())){
                scheduler.scheduleJob(jobDetail, createTrigger(cron));
            }
            scheduler.start();
        }catch (SchedulerException ex){
            throw new JobSystemException(ex);
        }
    }

    /**
     * 重新调度作业.
     *
     * @param cron CRON表达式
     */
    public synchronized void rescheduleJob(final String cron) {
        try {
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger && !cron.equals(trigger.getCronExpression())) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createTrigger(cron));
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 根据cron表达式 转换成trigger对象
     * @param cron cron表达式
     */
    private CronTrigger createTrigger(final String cron) {
        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(CronScheduleBuilder.cronSchedule(cron).withMisfireHandlingInstructionDoNothing()).build();
    }

    /**
     * 判断作业是否暂停
     */
    public synchronized boolean isPaused() {
        try {
            return !scheduler.isShutdown() && Trigger.TriggerState.PAUSED == scheduler.getTriggerState(new TriggerKey(triggerIdentity));
        } catch (SchedulerException e) {
            throw new JobSystemException(e);
        }

    }

    /**
     * 暂停作业.
     */
    public synchronized void pauseJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.pauseAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 恢复作业.
     */
    public synchronized void resumeJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.resumeAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 立刻启动作业.
     */
    public synchronized void triggerJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.triggerJob(jobDetail.getKey());
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 关闭调度器.
     */
    public synchronized void shutdown() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
}
