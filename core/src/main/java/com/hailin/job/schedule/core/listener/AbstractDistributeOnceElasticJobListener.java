
package com.hailin.job.schedule.core.listener;


import com.hailin.shrine.job.common.exception.JobSystemException;
import com.hailin.shrine.job.common.util.TimeService;
import com.hailin.job.schedule.core.basic.guarantee.GuaranteeService;
import com.hailin.job.schedule.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import lombok.Setter;

/**
 * 在分布式作业中只执行一次的监听器.
 * 
 * @author zhangliang
 */
public abstract class AbstractDistributeOnceElasticJobListener implements ElasticJobListener {
    
    private final long startedTimeoutMilliseconds;
    
    private final Object startedWait = new Object();
    
    private final long completedTimeoutMilliseconds;
    
    private final Object completedWait = new Object();
    
    @Setter
    private GuaranteeService guaranteeService;
    
    private TimeService timeService = new TimeService();
    
    public AbstractDistributeOnceElasticJobListener(final long startedTimeoutMilliseconds, final long completedTimeoutMilliseconds) {
        if (startedTimeoutMilliseconds <= 0L) {
            this.startedTimeoutMilliseconds = Long.MAX_VALUE;
        } else {
            this.startedTimeoutMilliseconds = startedTimeoutMilliseconds;
        }
        if (completedTimeoutMilliseconds <= 0L) {
            this.completedTimeoutMilliseconds = Long.MAX_VALUE; 
        } else {
            this.completedTimeoutMilliseconds = completedTimeoutMilliseconds;
        }
    }
    
    @Override
    public final void beforeJobExecuted(final JobExecutionMultipleShardingContext shardingContexts) {
        guaranteeService.registerStart(shardingContexts.getShardingItemParameters().keySet());
        if (guaranteeService.isAllStarted()) {
            doBeforeJobExecutedAtLastStarted(shardingContexts);
            guaranteeService.clearAllStartedInfo();
            return;
        }
        long before = timeService.getCurrentMillis();
        try {
            synchronized (startedWait) {
                startedWait.wait(startedTimeoutMilliseconds);
            }
        } catch (final InterruptedException ex) {
            Thread.interrupted();
        }
        if (timeService.getCurrentMillis() - before >= startedTimeoutMilliseconds) {
            guaranteeService.clearAllStartedInfo();
            handleTimeout(startedTimeoutMilliseconds);
        }
    }
    
    @Override
    public final void afterJobExecuted(final JobExecutionMultipleShardingContext shardingContexts) {
        guaranteeService.registerComplete(shardingContexts.getShardingItemParameters().keySet());
        if (guaranteeService.isAllCompleted()) {
            doAfterJobExecutedAtLastCompleted(shardingContexts);
            guaranteeService.clearAllCompletedInfo();
            return;
        }
        long before = timeService.getCurrentMillis();
        try {
            synchronized (completedWait) {
                completedWait.wait(completedTimeoutMilliseconds);
            }
        } catch (final InterruptedException ex) {
            Thread.interrupted();
        }
        if (timeService.getCurrentMillis() - before >= completedTimeoutMilliseconds) {
            guaranteeService.clearAllCompletedInfo();
            handleTimeout(completedTimeoutMilliseconds);
        }
    }
    
    private void handleTimeout(final long timeoutMilliseconds) {
        throw new JobSystemException("Job timeout. timeout mills is %s.", timeoutMilliseconds);
    }
    
    /**
     * 分布式环境中最后一个作业执行前的执行的方法.
     *
     * @param shardingContexts 分片上下文
     */
    public abstract void doBeforeJobExecutedAtLastStarted(JobExecutionMultipleShardingContext shardingContexts);
    
    /**
     * 分布式环境中最后一个作业执行后的执行的方法.
     *
     * @param shardingContexts 分片上下文
     */
    public abstract void doAfterJobExecutedAtLastCompleted(JobExecutionMultipleShardingContext shardingContexts);
    
    /**
     * 通知任务开始.
     */
    public void notifyWaitingTaskStart() {
        synchronized (startedWait) {
            startedWait.notifyAll();
        }
    }
    
    /**
     * 通知任务结束.
     */
    public void notifyWaitingTaskComplete() {
        synchronized (completedWait) {
            completedWait.notifyAll();
        }
    }
}
