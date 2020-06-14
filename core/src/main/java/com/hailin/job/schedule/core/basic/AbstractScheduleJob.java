package com.hailin.job.schedule.core.basic;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.hailin.job.schedule.core.basic.sharding.context.JobExecutionMultipleShardingContext;
import com.hailin.job.schedule.core.basic.statistics.ProcessCountStatistics;
import com.hailin.job.schedule.core.executor.ScheduleExecutorService;
import com.hailin.shrine.job.ScheduleJobReturn;
import com.hailin.shrine.job.ScheduleSystemErrorGroup;
import com.hailin.shrine.job.ScheduleSystemReturnCode;
import com.hailin.shrine.job.common.exception.JobException;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.PropertyPlaceholderHelper;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public abstract class AbstractScheduleJob extends AbstractElasticJob{

    private static final Logger log = LoggerFactory.getLogger(AbstractScheduleJob.class);

    protected static PropertyPlaceholderHelper placeHolderHelper = new PropertyPlaceholderHelper("{", "}");
    @Override
    protected final void executeJob(JobExecutionMultipleShardingContext shardingContext) {

        if (!(shardingContext instanceof ScheduleExecutionContext)){
            log.error( "!!! The context must be instance of SaturnJobExecutionContext !!!");
            return;
        }
        long start = System.currentTimeMillis();
        ScheduleExecutionContext scheduleContext = (ScheduleExecutionContext)shardingContext;
        scheduleContext.setSaturnJob(true);
        Map<Integer , ScheduleJobReturn> retMap = Maps.newHashMap();

        // 分片参数 解析成key/value值对
        Map<Integer , String> shardingItemParameters = scheduleContext.getShardingItemParameters();
        List<Integer> items = scheduleContext.getShardingItems();

        log.info("Job {} handle items : {}" , jobName , items);
        for (Integer item : items ) {
            // 兼容配置错误，如配置3个分片, 参数表配置为0=*, 2=*, 则1分片不会执行
            if (!shardingItemParameters.containsKey(item)){
                log.error("The {} item's parameter is not valid, will not execute the business code, please check shardingItemParameters",
                        items);
                ScheduleJobReturn errRet = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL,
                        "Config of parameter is not valid, check shardingItemParameters", ScheduleSystemErrorGroup.FAIL);
                retMap.put(item, errRet);
            }
        }
        Map<Integer , ScheduleJobReturn > handleJobMap = handleJob(scheduleContext);
        if (MapUtils.isNotEmpty(handleJobMap)){
            retMap.putAll(handleJobMap);
        }
        for (Integer item : items){
            if (Objects.isNull(item)){
                continue;
            }
            ScheduleJobReturn jobReturn = retMap.get(item);
            if (Objects.isNull(jobReturn)){
                jobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL , "Can not find the corresponding SaturnJobReturn", ScheduleSystemErrorGroup.FAIL);
                retMap.put(item , jobReturn);
            }
            updateExecuteResult(jobReturn, scheduleContext, item);
        }
        long end = System.currentTimeMillis();
        log.info("{} finished, totalCost={}ms, return={}", jobName, (end - start), retMap);
    }

    private void updateExecuteResult(ScheduleJobReturn jobReturn, ScheduleExecutionContext scheduleContext, Integer item) {
        int successCount = 0 , errorCount = 0;
        if (ScheduleSystemReturnCode.JOB_NO_COUNT != jobReturn.getReturnCode()){
            int errorGroup = jobReturn.getErrorGroup();
            if (errorGroup == ScheduleSystemErrorGroup.SUCCESS) {
                successCount ++ ;
            }else {
                if (errorGroup == ScheduleSystemErrorGroup.TIMEOUT){
                    onTimeout(item);
                }else if (errorGroup == ScheduleSystemErrorGroup.FAIL_NEED_RAISE_ALARM){
                    onNeedRaiseAlarm(item , jobReturn.getReturnMsg());
                }
                errorCount++;
            }
            // 展现分片处理失败的状态
            scheduleContext.getShardingItemResults().put(item , jobReturn);
            // 只要有出错和失败的分片 就是处理失败 否则认为处理成功
            if (errorCount == 0 && successCount >= 0) {
                ProcessCountStatistics.incrementProcessSuccessCount(executorName, jobName, successCount);
            } else {
                ProcessCountStatistics.increaseErrorCountDelta(executorName, jobName);
                ProcessCountStatistics.incrementProcessFailureCount(executorName, jobName, errorCount);
            }
        }


    }

    /**
     * 实际处理逻辑
     * @param scheduleContext 上下文
     * @return 每一个分片返回一个ScheduleJobReturn,若为null,表示执行失败
     */
    protected abstract Map<Integer, ScheduleJobReturn> handleJob(ScheduleExecutionContext scheduleContext);

    /**
     * 获取作业超时时间(秒)
     */
    public int getTimeoutSeconds() {
        return getConfigurationService().getTimeoutSeconds();
    }


    /**
     * 获取替换后的作业分片执行值
     * @param jobParameter 作业参数
     * @param jobValue 作业value
     * @return 替换后的值
     */
    protected String getRealItemValue(String jobParameter, String jobValue) {
        Properties kvProp = parseKV(jobParameter);
        int kvSize = Objects.isNull(kvProp)?0:kvProp.size();
        final String itemVal ; // 作业分片的对应值
        if (kvSize > 0){
            //有自定义的参数，解析完替换
            itemVal = placeHolderHelper.replacePlaceholders(jobValue, kvProp);
        }else {
            itemVal = jobValue;
        }
        return itemVal.replaceAll("!!", "\"").replaceAll("@@", "=").replaceAll("##", ",");
    }

    public Properties parseKV(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return null;
        }
        Properties kv = new Properties();
        String[] paths = path.split(",");
        if (paths.length > 0) {
            for (String p : paths) {
                String[] tmps = p.split("=");
                if (tmps.length == 2) {
                    kv.put(tmps[0].trim(), tmps[1].trim());
                } else {
                    log.warn( "job {} Param is not valid {}", jobName, p);
                }
            }
        }

        return kv;
    }

    protected abstract static class JobBusinessClassMethodCaller{

        public Object call(Object jobBusinessInstance, ScheduleExecutorService scheduleExecutorService) throws Exception {
            if (jobBusinessInstance == null) {
                throw new JobException("the job business instance is not initialized");
            }
            ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader jobClassLoader = scheduleExecutorService.getJobClassLoader();
            Thread.currentThread().setContextClassLoader(jobClassLoader);
            try {
                final Class<?> saturnJobExecutionContextClazz = jobClassLoader
                        .loadClass(ScheduleExecutorService.class.getCanonicalName());
                return internalCall(jobClassLoader, saturnJobExecutionContextClazz);
            } finally {
                Thread.currentThread().setContextClassLoader(oldClassLoader);
            }
        }

        protected abstract Object internalCall(ClassLoader jobClassLoader, Class<?> saturnJobExecutionContextClazz)
                throws Exception;
    }

    public String logBusinessExceptionIfNecessary(String jobName, Throwable t) {
        String message = null;
        if (t instanceof ReflectiveOperationException) {
            Throwable cause = t.getCause();
            if (cause != null) {
                message = cause.toString();
            }
        }
        if (message == null) {
            message = t.toString();
        }
        log.error(jobName + message, t);
        return message;
    }
}
