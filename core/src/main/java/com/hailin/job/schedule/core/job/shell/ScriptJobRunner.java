package com.hailin.job.schedule.core.job.shell;

import com.google.common.collect.Maps;
import com.hailin.job.schedule.core.basic.AbstractScheduleJob;
import com.hailin.job.schedule.core.basic.ScheduleExecutionContext;
import com.hailin.job.schedule.core.job.constant.ShrineConstant;
import com.hailin.job.schedule.core.utils.ScriptPidUtils;
import com.hailin.shrine.job.ScheduleJobReturn;
import com.hailin.shrine.job.ScheduleSystemErrorGroup;
import com.hailin.shrine.job.ScheduleSystemReturnCode;
import com.hailin.shrine.job.common.util.JsonUtils;
import com.hailin.shrine.job.common.util.SystemEnvProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
public class ScriptJobRunner {

    private static Logger log = LoggerFactory.getLogger(ScriptJobRunner.class);

    private static final String PREFIX_COMAND = " source /etc/profile; ";

    private Map<String, String> envMap = new HashMap<>();

    private AbstractScheduleJob job;

    private Integer item;

    private String itemValue;

    private ScheduleExecutionContext context;

    private String jobName;

    private ScheduleExecuteWatchdog watchdog;

    private boolean businessReturned = false;

    private File scheduleOutputFile;

    public ScriptJobRunner(Map<String, String> envMap, AbstractScheduleJob job, Integer item, String itemValue,
                           ScheduleExecutionContext context) {
        if (envMap != null) {
            this.envMap.putAll(envMap);
        }
        this.job = job;
        this.item = item;
        this.itemValue = itemValue;
        this.context = context;
        if (job != null) {
            this.jobName = job.getJobName();
        }
    }

    public ScheduleJobReturn runJob() {
        ScheduleJobReturn result = null;
        long timeoutSeconds = context.getTimetoutSeconds();
        try {
            createScheduleJobReturnFile();
            result = execute(timeoutSeconds);
        }catch (Throwable t){
            log.error( "{} - {} Exception", jobName, item, t);
            result = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL, "Exception: " + t,
                    ScheduleSystemErrorGroup.FAIL);
        }finally {
            FileUtils.deleteQuietly(scheduleOutputFile.getParentFile());
        }
        if (Objects.isNull(result.getProp())){
            result.setProp(Maps.newHashMap());
        }
        return result;
    }

    private ScheduleJobReturn readScheduleJobReturn() {
        ScheduleJobReturn tmp = null;
        if (scheduleOutputFile != null && scheduleOutputFile.exists()) {
            try {
                String fileContents = FileUtils.readFileToString(scheduleOutputFile);
                if (StringUtils.isNotBlank(fileContents)) {
                    tmp = JsonUtils.getGson().fromJson(fileContents.trim(), ScheduleJobReturn.class);
                    businessReturned = true; // 脚本成功返回数据
                }
            } catch (Throwable t) {
                log.error("{} - {} read SaturnJobReturn from {} error", jobName, item,
                        scheduleOutputFile.getAbsolutePath(), t);
                tmp = new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL, "Exception: " + t,
                        ScheduleSystemErrorGroup.FAIL);
            }
        }
        return tmp;
    }

    private ScheduleJobReturn execute(long timeoutSeconds) {
        ScheduleJobReturn scheduleJobReturn ;
        ProcessOutputStream  processOutputStream = new ProcessOutputStream(1);
        DefaultExecutor executor = new DefaultExecutor();
        PumpStreamHandler streamHandler = new PumpStreamHandler(processOutputStream);
        streamHandler.setStopTimeout(timeoutSeconds * 1000);// 关闭线程等待时间, (注意commons-exec会固定增加2秒的addition)
        executor.setExitValue(0);
        executor.setStreamHandler(streamHandler);
        executor.setWatchdog(getWatchdog());
        Map<String, String> env = ScriptPidUtils.loadEnv();
        CommandLine commandLine = createCommandLine(env);

        try {
            long start = System.currentTimeMillis();
            log.info( "Begin executing {}-{} {}", jobName, item, commandLine);
            int exitValue = executor.execute(commandLine, env);
            long end = System.currentTimeMillis();
            log.info( "Finish executing {}-{} {}, the exit value is {}, cost={}ms", jobName, item,
                    commandLine, exitValue, (end - start));

            ScheduleJobReturn tmp = readScheduleJobReturn();
            if (tmp == null) {
                tmp = new ScheduleJobReturn("the exit value is " + exitValue);
            }
            scheduleJobReturn = tmp;
        } catch (Exception e) {
            scheduleJobReturn = handleException(timeoutSeconds, e);
        } finally {
            try {
                // 将日志set进jobLog, 写不写zk再由ExecutionService控制
                handleJobLog(processOutputStream.getJobLog());
                processOutputStream.close();
            } catch (Exception ex) {
                log.error( "{}-{} Error at closing output stream. Should not be concern: {}", jobName,
                        item, ex.getMessage(), ex);
            }
            stopStreamHandler(streamHandler);
            ScriptPidUtils.removePidFile(job.getExecutorName(), jobName, "" + item, watchdog.getPid());
        }
        return scheduleJobReturn;
    }

    private CommandLine createCommandLine(Map<String, String> env) {
        StringBuilder envStringBuilder = new StringBuilder();
        if (envMap != null && !envMap.isEmpty()) {
            for (Map.Entry<String, String> envEntrySet : envMap.entrySet()) {
                envStringBuilder.append("export ").append(envEntrySet.getKey()).append('=')
                        .append(envEntrySet.getValue()).append(';');
            }
        }
        String execParameter =
                envStringBuilder.toString() + PREFIX_COMAND + ScriptPidUtils.filterEnvInCmdStr(env, itemValue);
        final CommandLine cmdLine = new CommandLine("/bin/sh");
        cmdLine.addArguments(new String[]{"-c", execParameter}, false);
        return cmdLine;
    }

    private void handleJobLog(String jobLog) {
        //出于系统保护考虑 ，jobLog不能超过1M
        if (jobLog != null && jobLog.length() > ShrineConstant.MAX_JOB_LOG_DATA_LENGTH) {
            log.info("As the job log exceed max length, only the previous {} characters will be reported",
                    ShrineConstant.MAX_JOB_LOG_DATA_LENGTH);
            jobLog = jobLog.substring(0, ShrineConstant.MAX_JOB_LOG_DATA_LENGTH);
        }

        context.putJobLog(item, jobLog);

        // 提供给saturn-job-executor.log日志输出shell命令jobLog，以后若改为重定向到日志，则可删除此输出
        System.out.println("[" + jobName + "] msg=" + jobName + "-" + item + ":" + jobLog);// NOSONAR

        log.info( "{}-{}: {}", jobName, item, jobLog);
    }

    private void stopStreamHandler(PumpStreamHandler streamHandler) {
        try {
            streamHandler.stop();
        } catch (IOException ex) {
            log.debug("{}-{} Error at closing log stream. Should not be concern: {}", jobName, item,
                    ex.getMessage(), ex);
        }
    }

    private void createScheduleJobReturnFile() throws IOException {
        if (envMap.containsKey(SystemEnvProperties.NAME_SCHEDULE_OUTPUT_PATH)) {
            String saturnOutputPath = envMap.get(SystemEnvProperties.NAME_SCHEDULE_OUTPUT_PATH);
            scheduleOutputFile = new File(saturnOutputPath);
            if (!scheduleOutputFile.exists()) {
                FileUtils.forceMkdir(scheduleOutputFile.getParentFile());
                if (!scheduleOutputFile.createNewFile()) {
                    log.warn( "file {} already exsits.", saturnOutputPath);
                }
            }
        }
    }

    private ScheduleJobReturn handleException(long timeoutSeconds, Exception e) {
        ScheduleJobReturn saturnJobReturn;
        String errMsg = e.toString();
        if (watchdog.isTimeout()) {
            saturnJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL,
                    String.format("execute job timeout(%sms), %s", timeoutSeconds * 1000, errMsg),
                    ScheduleSystemErrorGroup.TIMEOUT);
            log.error( "{}-{} timeout, {}", jobName, item, errMsg);
            return saturnJobReturn;
        }

        if (watchdog.isForceStop()) {
            saturnJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.SYSTEM_FAIL,
                    "the job was forced to stop, " + errMsg, ScheduleSystemErrorGroup.FAIL);
            log.error( "{}-{} force stopped, {}", jobName, item, errMsg);
            return saturnJobReturn;
        }

        saturnJobReturn = new ScheduleJobReturn(ScheduleSystemReturnCode.USER_FAIL, "Exception: " + errMsg,
                ScheduleSystemErrorGroup.FAIL);
        log.error( "{}-{} Exception: {}", jobName, item, errMsg, e);
        return saturnJobReturn;
    }
}
