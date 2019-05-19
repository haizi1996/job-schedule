package com.hailin.shrine.job.core.executor;

import com.hailin.shrine.job.ShrineJobReturn;
import com.hailin.shrine.job.core.basic.execution.ExecutionContextService;
import com.hailin.shrine.job.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.shrine.job.core.job.config.JobConfiguration;
import com.hailin.shrine.job.core.job.event.type.JobExecutionEvent;
import com.hailin.shrine.job.core.job.executor.handler.JobExceptionHandler;
import com.hailin.shrine.job.core.strategy.JobScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * 弹性化分布式作业执行器.
 *
 * @author zhanghailin
 */
public abstract class AbstractElasticJobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractElasticJobExecutor.class);

    private JobScheduler jobScheduler;

    private JobConfiguration jobConfiguration;

    private String jobName;

    private ExecutorService executorService;

    private JobExceptionHandler jobExceptionHandler;

    private Map<Integer, ShrineJobReturn> jobReturnMap;

    private ExecutionContextService executionContextService;

    /**
     * 执行作业
     */
    public final void execute() {
        //检查作业环境  ，校验时间与注册中心的时间是否在某个误差内

        //获取分片上下文
        JobExecutionMultipleShardingContext shardingContext = executionContextService.getJobExecutionMultipleShardingContext(null);
        //发送追踪时间

        try {
            process(shardingContext, JobExecutionEvent.ExecutionSource.NORMAL_TRIGGER);
        } finally {

        }


    }

    private void process(JobExecutionMultipleShardingContext shardingContexts, JobExecutionEvent.ExecutionSource executionSource) {
        List<Integer> items = shardingContexts.getShardingItems();
        if (items.size() == 1) {
            int item = shardingContexts.getShardingItemParameters().entrySet().iterator().next().getKey();

            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(shardingContexts.getTaskId(), jobName, executionSource, item);
            process(shardingContexts, item, jobExecutionEvent);
        }

        final CountDownLatch latch = new CountDownLatch(items.size());

        for (final int each : items) {
            final JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(shardingContexts.getTaskId(), jobName, executionSource, each);
            if (executorService.isShutdown()) {
                return;
            }
            executorService.submit(() -> {
                try {
                    process(shardingContexts, each, jobExecutionEvent);
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(JobExecutionMultipleShardingContext shardingContext, int item, JobExecutionEvent jobExecutionEvent) {
        // 发送作业时间

        LOGGER.trace("Job '{}' executing, item is: '{}'.", jobName, item);
        JobExecutionEvent completeEvent;

        try {
            process(new ShardingContext(shardingContext, item));
            //加入一个超时任务
        } catch (final Throwable cause) {

        }

    }

    /**
     * 处理单个作业分片
     *
     * @param shardingContext 这个分片上下文
     */
    protected abstract void process(ShardingContext shardingContext);

}
