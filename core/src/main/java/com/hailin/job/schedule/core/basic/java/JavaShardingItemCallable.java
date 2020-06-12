package com.hailin.job.schedule.core.basic.java;

import com.hailin.job.schedule.core.basic.AbstractScheduleJob;
import com.hailin.job.schedule.core.basic.ScheduleExecutionContext;
import com.hailin.job.schedule.core.basic.ScheduleJavaJob;
import com.hailin.job.schedule.core.basic.ShardingItemCallable;
import com.hailin.job.schedule.core.job.ScheduleJobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

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
            log.error( jobName + t.toString(), t);
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
}
