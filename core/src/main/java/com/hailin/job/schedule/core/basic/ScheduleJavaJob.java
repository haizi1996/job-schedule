package com.hailin.job.schedule.core.basic;

import com.google.common.collect.Maps;
import com.hailin.job.schedule.core.basic.java.JavaShardingItemCallable;
import com.hailin.job.schedule.core.basic.java.ShardingItemFutureTask;
import com.hailin.job.schedule.core.basic.java.TimeoutSchedulerExecutor;
import com.hailin.shrine.job.ScheduleJobReturn;
import com.hailin.shrine.job.ScheduleSystemErrorGroup;
import com.hailin.shrine.job.ScheduleSystemReturnCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
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
                        TimeoutSchedulerExecutor.scheduleTimeoutJob(scheduleContext.getExecutorName() , timeoutSeconds , shardingItemFutureTask);
                    }
                    shardingItemFutureTask.setCallFuture(callFuture);
                    futureTaskMap.put(key , shardingItemFutureTask);
                }catch (Throwable t){
                    log.error( t.getMessage(), t);
                    retMap.put(key, new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL, t.getMessage(),
                            ScheduleSystemErrorGroup.FAIL));
                }

            }
        }

        for (Map.Entry<Integer, ShardingItemFutureTask> entry : futureTaskMap.entrySet()) {
            Integer item = entry.getKey();
            ShardingItemFutureTask futureTask = entry.getValue();
            try {
                futureTask.getCallFuture().get();
            }catch (Exception e){
                log.error( e.getMessage(), e);
                retMap.put(item, new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL, e.getMessage(),
                        ScheduleSystemErrorGroup.FAIL));
                continue;
            }
            retMap.put(item , futureTask.getCallable().getScheduleJobReturn());
        }
        synchronized (futureTaskMap){
            futureTaskMap.clear();
        }
        return retMap;
    }

    public JavaShardingItemCallable createCallable(String jobName, Integer item, String itemVal, int timeoutSeconds, ScheduleExecutionContext scheduleContext, AbstractScheduleJob  scheduleJob) {
        return new JavaShardingItemCallable(jobName , item , itemVal ,timeoutSeconds ,scheduleContext ,scheduleJob);
    }


    @Override
    public void onForceStop(int item) {

    }

    public ScheduleJobReturn doExecution(final String jobName , final  Integer key , final String value , ScheduleExecutionContext context , final JavaShardingItemCallable callable ) throws Throwable {
        String jobClass = context.getJobConfiguration().getJobClass();
        log.info("Running SaturnJavaJob,  jobClass [{}], item [{}]", jobClass, key);
        try {
            Object ret = new JobBusinessClassMethodCaller() {
                @Override
                protected Object internalCall(ClassLoader jobClassLoader, Class<?> saturnJobExecutionContextClazz) throws Exception {
                    return jobBusinessInstance.getClass().getMethod("handleJvaJob" , String.class , Integer.class , String.class , saturnJobExecutionContextClazz).invoke(jobBusinessInstance,jobName , key , value , callable.getContextForJob(jobClassLoader));
                }
            }.call(jobBusinessInstance ,scheduleExecutorService);
             ScheduleJobReturn scheduleJobReturn = (ScheduleJobReturn) JavaShardingItemCallable.cloneObject(ret , scheduleExecutorService.getExecutorClassLoader()) ;
            if (Objects.nonNull(scheduleJobReturn)){
                callable.setBusinessReturned(true);
            }
            return scheduleJobReturn;
        }catch (Exception e){
            if (e.getCause() instanceof ThreadDeath) {
                throw e.getCause();
            }
            String message = logBusinessExceptionIfNecessary(jobName, e);
            return new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL, message, ScheduleSystemErrorGroup.FAIL);
        }
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
    public void postTimeout(final String jobName, final Integer key, final String value,
                            ScheduleExecutionContext shardingContext, final JavaShardingItemCallable callable) {
        callJobBusinessClassMethodTimeoutOrForceStop(jobName, shardingContext, callable, "onTimeout", key, value);
    }



    public void beforeForceStop(final String jobName, final Integer key, final String value,
                                ScheduleExecutionContext shardingContext, final JavaShardingItemCallable callable) {
        callJobBusinessClassMethodTimeoutOrForceStop(jobName, shardingContext, callable, "beforeForceStop", key, value);
    }

    public void postForceStop(final String jobName, final Integer key, final String value,
                              ScheduleExecutionContext shardingContext, final JavaShardingItemCallable callable) {
        callJobBusinessClassMethodTimeoutOrForceStop(jobName, shardingContext, callable, "postForceStop", key, value);
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
