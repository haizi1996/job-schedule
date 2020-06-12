package com.hailin.job.schedule.core.basic;

import com.google.common.collect.Maps;
import com.hailin.job.schedule.core.basic.java.JavaShardingItemCallable;
import com.hailin.job.schedule.core.basic.java.ShardingItemFutureTask;
import com.hailin.shrine.job.ScheduleJobReturn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ScheduleJavaJob extends AbstractScheduleJob {


    private static Logger log = LoggerFactory.getLogger(ScheduleJavaJob.class);

    private Map<Integer, ShardingItemFutureTask> futureTaskMap = Maps.newHashMap();

    private Object jobBusinessInstance = null;

    @Override
    protected Map<Integer, ScheduleJobReturn> handleJob(ScheduleExecutionContext scheduleContext) {

        final Map<Integer , ScheduleJobReturn> retMap = Maps.newHashMap();
        synchronized (futureTaskMap){
            futureTaskMap.clear();
            final String jobName = scheduleContext.getJobName();
            final int timeoutSeconds = getTimeoutSeconds();
            ExecutorService executorService = getExecutorService();

            // 处理自定义参数
            String jobParameter = scheduleContext.getJobParameter();
            // shardingItemParameters为参数表解析出来的Key/Value值
            Map<Integer,String> shardingItemParameters = scheduleContext.getShardingItemParameters();

            for (final Map.Entry<Integer, String> shardingItem : shardingItemParameters.entrySet() ){
                final Integer key = shardingItem.getKey();
                try {
                    String jobValue = shardingItem.getValue();
                    final String itemVal = getRealItemValue(jobParameter , jobValue);
                    ShardingItemFutureTask shardingItemFutureTask = new ShardingItemFutureTask(createCallable(jobName , key , itemVal , timeoutSeconds , scheduleContext ,this) , null);
                    Future<?> callFuture = executorService.submit(shardingItemFutureTask);
                    if (timeoutSeconds > 0){

                    }
                }

            }
        }

        return null;
    }

    public JavaShardingItemCallable createCallable(String jobName, Integer item, String itemVal, int timeoutSeconds, ScheduleExecutionContext scheduleContext, AbstractScheduleJob  scheduleJob) {
        return new JavaShardingItemCallable(jobName , item , itemVal ,timeoutSeconds ,scheduleContext ,scheduleJob);
    }


    @Override
    public void onForceStop(int item) {

    }

    @Override
    public void onTimeout(int item) {

    }

    @Override
    public void onNeedRaiseAlarm(int item, String alarmMessage) {

    }

    public void beforeTimeout(final String jobName, final Integer key, final String value,
                              ScheduleExecutionContext shardingContext, final JavaShardingItemCallable callable) {
        callJobBusinessClassMethodTimeoutOrForceStop(jobName, shardingContext, callable, "beforeTimeout", key, value);
    }

    private void callJobBusinessClassMethodTimeoutOrForceStop(String jobName, ScheduleExecutionContext shardingContext, JavaShardingItemCallable callable, String methodName, Integer key, String value) {
        String jobClass = shardingContext.getJobConfiguration().getJobClass();
        log.info( "SaturnJavaJob {},  jobClass is {}", methodName, jobClass);
        try {
            new JobBusinessClassMethodCaller() {
                @Override
                protected Object internalCall(ClassLoader jobClassLoader, Class<?> saturnJobExecutionContextClazz)
                        throws Exception {
                    return jobBusinessInstance.getClass()
                            .getMethod(methodName, String.class, Integer.class, String.class,
                                    saturnJobExecutionContextClazz).invoke(jobBusinessInstance, jobName, key, value,
                                    callable.getContextForJob(jobClassLoader));                }
            }.call(jobBusinessInstance, scheduleExecutorService);
        } catch (Exception e) {
            logBusinessExceptionIfNecessary(jobName, e);
        }
    }


}
