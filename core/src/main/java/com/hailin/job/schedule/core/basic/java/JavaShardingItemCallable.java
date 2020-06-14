package com.hailin.job.schedule.core.basic.java;

import com.hailin.job.schedule.core.basic.AbstractScheduleJob;
import com.hailin.job.schedule.core.basic.ScheduleExecutionContext;
import com.hailin.job.schedule.core.basic.ScheduleJavaJob;
import com.hailin.job.schedule.core.basic.ShardingItemCallable;
import com.hailin.job.schedule.core.job.ScheduleJobExecutionContext;
import com.hailin.job.schedule.core.job.constant.ShrineConstant;
import com.hailin.job.schedule.core.utils.ScheduleSystemOutputStream;
import com.hailin.shrine.job.ScheduleJobReturn;
import com.hailin.shrine.job.ScheduleSystemErrorGroup;
import com.hailin.shrine.job.ScheduleSystemReturnCode;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class JavaShardingItemCallable extends ShardingItemCallable {

    protected static final int INIT = 0;
    protected static final int TIMEOUT = 1;
    protected static final int SUCCESS = 2;
    protected static final int FORCE_STOP = 3;
    protected static final int STOPPED = 4;
    private static final Logger log = LoggerFactory.getLogger(JavaShardingItemCallable.class);
    protected Thread currentThread;
    protected AtomicInteger status = new AtomicInteger(INIT);
    protected boolean breakForceStop = false;
    protected Object contextForJob;

    public JavaShardingItemCallable(String jobName, Integer item, String itemValue, int timeoutSeconds, ScheduleExecutionContext shardingContext, AbstractScheduleJob scheduleJob) {
        super(jobName, item, itemValue, timeoutSeconds, shardingContext, scheduleJob);
    }

    /**
     * 复制对象
     */
    public static Object cloneObject(Object source, ClassLoader classLoader) throws Exception {
        if (source == null) {
            return null;
        }
        Class<?> clazz = classLoader.loadClass(source.getClass().getCanonicalName());
        Object target = clazz.newInstance();
        clazz.getMethod("copyFrom", Object.class).invoke(target, source);
        return target;
    }

    /**
     * 设置该分片的状态为TIMEOUT
     *
     * @return Mark timeout success or fail
     */
    public boolean setTimeout() {
        return status.compareAndSet(INIT, TIMEOUT);
    }

    /**
     * 该分片执行是否TIMEOUT
     */
    public boolean isTimeout() {
        return status.get() == TIMEOUT;
    }

    /**
     * 设置该分片的状态为FORCE_STOP
     */
    public boolean forceStop() {
        return status.compareAndSet(INIT, FORCE_STOP);
    }

    /**
     * 作业执行是否被中止
     */
    public boolean isBreakForceStop() {
        return breakForceStop;
    }

    /**
     * 该分片是否FORCE_STOP状态
     */
    public boolean isForceStop() {
        return status.get() == FORCE_STOP;
    }

    /**
     * 是否成功
     */
    public boolean isSuccess() {
        return status.get() == SUCCESS;
    }

    /**
     * 重新初始化
     */
    public void reset() {
        status.set(INIT);
        breakForceStop = false;
        scheduleJobReturn = null;
        businessReturned = false;
    }

    /**
     * 执行前回调
     */
    public void beforeExecution() {
        this.startTime = System.currentTimeMillis();
    }

    public void beforeTimeout() {
        try {
            ((ScheduleJavaJob) scheduleJob).beforeTimeout(jobName, item, itemValue, shardingContext, this);
        } catch (Throwable t) {
            log.error(jobName + t.toString(), t);
        }
    }

    public long getExecutionTime() {
        return endTime - startTime;
    }

    protected void onTimeout() {
        try {
            ((ScheduleJavaJob) scheduleJob).postTimeout(jobName, item, itemValue, shardingContext, this);
        } catch (Throwable t) {
            log.error(t.toString(), t);
        }
    }

    public void beforeForceStop() {
        try {
            ((ScheduleJavaJob) scheduleJob).beforeForceStop(jobName, item, itemValue, shardingContext, this);
        } catch (Throwable t) {
            log.error(t.toString(), t);
        }
    }

    protected void postForceStop() {
        try {
            ((ScheduleJavaJob) scheduleJob).postForceStop(jobName, item, itemValue, shardingContext, this);
        } catch (Throwable t) {
            log.error(t.toString(), t);
        }
    }

    /**
     * 生成分片上下文对象
     */
    public Object getContextForJob(ClassLoader jobClassLoader) throws Exception {
        if (contextForJob == null) {
            if (shardingContext == null) {
                return null;
            }
            ScheduleJobExecutionContext context = new ScheduleJobExecutionContext();
            context.setJobName(shardingContext.getJobName());
            context.setShardingItemParameters(shardingContext.getShardingItemParameters());
            context.setCustomContext(shardingContext.getCustomContext());
            context.setJobParameter(shardingContext.getJobParameter());
            context.setShardingItems(shardingContext.getShardingItems());
            context.setShardingTotalCount(shardingContext.getShardingTotalCount());
            context.setQueueName(shardingContext.getJobConfiguration().getQueueName());
            contextForJob = cloneObject(context, jobClassLoader);
        }

        return contextForJob;
    }

    public ScheduleJobReturn call() {
        reset();

        currentThread = Thread.currentThread();
        ScheduleJobReturn temp = null;
        try {

            beforeExecution();

            temp = doExecution();

            // 在此之后，不能再强制停止本线程
            breakForceStop = true;
        } catch (Throwable t) {
            // 在此之后，不能再强制停止本线程
            breakForceStop = true;

            // 不是超时，不是强制停止。 打印错误日志，设置SaturnJobReturn。
            if (status.get() != TIMEOUT && status.get() != FORCE_STOP) {
                log.error(t.toString(), t);
                temp = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL, t.getMessage(),
                        ScheduleSystemErrorGroup.FAIL);
            }

        } finally {
            if (status.compareAndSet(INIT, SUCCESS)) {
                scheduleJobReturn = temp;
            }
            if (Objects.nonNull(scheduleJob) && scheduleJob.getConfigurationService().showNormalLog()) {
                String jobLog = ScheduleSystemOutputStream.clearAndGetLog();
                if (Objects.nonNull(jobLog) && jobLog.length() > ShrineConstant.MAX_JOB_LOG_DATA_LENGTH) {
                    log.info("As the job log exceed max length, only the previous {} characters will be reported",
                            ShrineConstant.MAX_JOB_LOG_DATA_LENGTH);
                    jobLog = jobLog.substring(0, ShrineConstant.MAX_JOB_LOG_DATA_LENGTH);
                }
                this.shardingContext.putJobLog(this.item, jobLog);
            }
        }
        return scheduleJobReturn;
    }

    public ScheduleJobReturn doExecution() throws Throwable {
        return ((ScheduleJavaJob) scheduleJob).doExecution(jobName, item, itemValue, shardingContext, this);
    }

    protected void checkAndSetSaturnJobReturn() {
        switch (status.get()) {
            case TIMEOUT:
                scheduleJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL,
                        "execute job timeout(" + timeoutSeconds * 1000 + "ms)", ScheduleSystemErrorGroup.TIMEOUT);
                break;
            case FORCE_STOP:
                scheduleJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL, "the job was forced to stop",
                        ScheduleSystemErrorGroup.FAIL);
                break;
            case STOPPED:
                scheduleJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL,
                        "the job was stopped, will not run the business code", ScheduleSystemErrorGroup.FAIL);
                break;
            default:
                break;
        }
        if (scheduleJobReturn == null) {
            scheduleJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL,
                    "the SaturnJobReturn can not be null", ScheduleSystemErrorGroup.FAIL);
        }
    }

    public void afterExecution() {
        this.endTime = System.currentTimeMillis();
    }
}
