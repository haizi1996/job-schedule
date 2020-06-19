package com.hailin.job.schedule.core.job.shell;

import com.hailin.job.schedule.core.basic.AbstractScheduleJob;
import com.hailin.job.schedule.core.basic.ScheduleExecutionContext;
import com.hailin.job.schedule.core.basic.ShardingItemCallable;
import com.hailin.job.schedule.core.utils.ScriptPidUtils;
import com.hailin.shrine.job.ScheduleJobReturn;
import com.hailin.shrine.job.ScheduleSystemErrorGroup;
import com.hailin.shrine.job.ScheduleSystemReturnCode;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;


/**
 * 处理通用Script的调度
 */
public class ScheduleScriptJob extends AbstractScheduleJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleScriptJob.class);

    private Object watchDogLock = new Object();

    protected List<ScheduleExecuteWatchdog> watchDogList = new ArrayList<ScheduleExecuteWatchdog>();
    protected List<ShardingItemCallable> shardingItemCallableList = new ArrayList<>();

    private Random random = new Random();

    @Override
    protected Map<Integer, ScheduleJobReturn> handleJob(ScheduleExecutionContext shardingContext) {
        synchronized (watchDogList){
            watchDogList.clear();
        }
        shardingItemCallableList.clear();
        final Map<Integer, ScheduleJobReturn> retMap = new ConcurrentHashMap<>();

        Map<Integer, String> shardingItemParameters = shardingContext.getShardingItemParameters();

        final String jobName = shardingContext.getJobName();

        ExecutorService executorService = getExecutorService();

        // 处理自定义参数
        String jobParameter = shardingContext.getJobParameter();

        final CountDownLatch latch = new CountDownLatch(shardingItemParameters.size());
        for (final Map.Entry<Integer , String> shardingItem : shardingItemParameters.entrySet()){
            final Integer key = shardingItem.getKey();
            String jobValue = shardingItem.getValue();

            final String execParameter = getRealItemValue(jobParameter, jobValue); // 作业分片的对应值

            LOGGER.debug( "jobname={}, key= {}, jobParameter={}", jobName, key, execParameter);
            executorService.submit(()-> {
                    ScheduleJobReturn jobReturn = null;
                    try {
                        jobReturn = innerHandleWithListener(jobName, key, execParameter, shardingContext);
                    } catch (Throwable e) {
                        LOGGER.error( e.getMessage(), e);
                        jobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL, "Error: " + e.getMessage(),
                                ScheduleSystemErrorGroup.FAIL);
                    } finally {
                        retMap.put(key, jobReturn);
                        latch.countDown();
                    }
                });
        }
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            LOGGER.error( "SaturnScriptJob: Job {} is interrupted", jobName);
            Thread.currentThread().interrupt();
        }

        return retMap;
    }

    private ScheduleJobReturn innerHandleWithListener(String jobName, Integer item, String execParameter, ScheduleExecutionContext shardingContext) {
        ShardingItemCallable callable = createShardingItemCallable(jobName, item, execParameter, shardingContext);
        shardingItemCallableList.add(callable);

        beforeExecution(callable);

        ScheduleJobReturn saturnJobReturn = null;
        try {
            saturnJobReturn = innerHandle(callable);
        } catch (Throwable t) {
            LOGGER.error( t.getMessage(), t);
            saturnJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL, t.getMessage(),
                    ScheduleSystemErrorGroup.FAIL);
        }

        callable.setScheduleJobReturn(saturnJobReturn);
        afterExecution(callable);

        LOGGER.debug( "job:{} item:{} finish execution, which takes {}ms", jobName, item,
                callable.getExecutionTime());

        return saturnJobReturn;
    }

    protected ScheduleJobReturn innerHandle(ShardingItemCallable callable) {
        ScheduleJobReturn res = null;
        try {
            String saturnOutputPath = String
                    .format(ScriptPidUtils.JOBITEMOUTPUTPATH, callable.getShardingContext().getExecutorName(), jobName,
                            callable.getItem(), random.nextInt(10000), System.currentTimeMillis());
            callable.getEnvMap().put(SystemEnvProperties.NAME_SCHEDULE_OUTPUT_PATH, saturnOutputPath);

            ScriptJobRunner scriptJobRunner = new ScriptJobRunner(callable.getEnvMap(), this, callable.getItem(),
                    callable.getItemValue(), callable.getShardingContext());
            ScheduleExecuteWatchdog watchDog = scriptJobRunner.getWatchdog();
            synchronized (watchDogList) {
                watchDogList.add(watchDog);
            }
            res = scriptJobRunner.runJob();
            synchronized (watchDogLock) {
                watchDogList.remove(watchDog);
            }
            callable.setBusinessReturned(scriptJobRunner.isBusinessReturned());
        }catch (Throwable t){
            LOGGER.error(t.getMessage() , t);
            res = new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL , t.getMessage() , ScheduleSystemErrorGroup.FAIL);
        }
        return res;

    }

    public ShardingItemCallable createShardingItemCallable(String jobName, Integer item, String execParameter,
                                                           ScheduleExecutionContext shardingContext) {
        ShardingItemCallable callable = new ShardingItemCallable(jobName, item, execParameter, getTimeoutSeconds(),
                shardingContext, this);
        return callable;
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

    public void beforeExecution(ShardingItemCallable callable) {
        callable.setStartTime(System.currentTimeMillis());
    }

    public void afterExecution(ShardingItemCallable callable) {
        callable.setEndTime(System.currentTimeMillis());
    }

    @Override
    public void forceStop() {
        super.forceStop();
        super.forceStop();
        LOGGER.info( "shell executor invoked forceStop, watchDogList = {}", watchDogList);
        if (watchDogList == null || watchDogList.isEmpty()) {
            ScriptPidUtils.forceStopRunningShellJob(executorName, jobName);
        } else {
            List<ScheduleExecuteWatchdog> tmp = new ArrayList<ScheduleExecuteWatchdog>();
            synchronized (watchDogLock) {
                tmp.addAll(watchDogList);
            }

            for (ScheduleExecuteWatchdog watchDog : tmp) {
                LOGGER.info( "Job {}-{} is stopped, force the script {} to exit.", watchDog.getJobName(),
                        watchDog.getJobItem(), watchDog.getExecParam());
                // kill process and stop watchdog, mark forceStop
                // it will use kill, but not kill -9.
                watchDog.destroyProcess();

                // use kill -9
                int jobItem = watchDog.getJobItem();
                long pid = ScriptPidUtils.getFirstPidFromFile(serverService.getExecutorName(), watchDog.getJobName(),
                        "" + Integer.toString(jobItem));
                if (pid > 0 && ScriptPidUtils.isPidRunning(pid)) {
                    ScriptPidUtils.killAllChildrenByPid(pid, true);
                }

                // remove pid files
                ScriptPidUtils.removeAllPidFile(serverService.getExecutorName(), watchDog.getJobName(), jobItem);

                onForceStop(jobItem);
            }
        }
    }
}
