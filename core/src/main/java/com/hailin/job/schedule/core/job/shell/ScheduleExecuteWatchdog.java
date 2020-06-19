package com.hailin.job.schedule.core.job.shell;

import com.hailin.job.schedule.core.utils.ScriptPidUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.OS;
import org.apache.commons.exec.Watchdog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class ScheduleExecuteWatchdog extends ExecuteWatchdog {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleExecuteWatchdog.class);


    // 四种状态
    private static final int INIT = 0;

    private static final int TIMEOUT = 1;

    private static final int FORCE_STOP = 2;

    private static final int INVALID_PID = -1;

    //  作业名称
    private String jobName;

    // 作业分片项
    private int jobItem;

    // 执行参数
    private String execParam;

    // 执行器名称
    private String executorName;

    private long pid = -1;

    private AtomicInteger status = new AtomicInteger(INIT);

    private Process monitoringProcess;

    private boolean hasKilledProcess;

    public ScheduleExecuteWatchdog(long timeout, String jobName, int jobItem, String execParam, String executorName) {
        super(timeout);
        this.jobName = jobName;
        this.jobItem = jobItem;
        this.execParam = execParam;
        this.executorName = executorName;
    }

    @Override
    public synchronized void start(final Process processToMonitor) {
        super.start(processToMonitor);
        this.monitoringProcess = processToMonitor;

        // get and save pid to file
        pid = getPidByProcess(monitoringProcess);
        if (hasValidPid()) {
            ScriptPidUtils.writePidToFile(executorName, jobName, jobItem, pid);
        }
    }

    @Override
    public synchronized void destroyProcess() {
        status.compareAndSet(INIT , FORCE_STOP);
        super.destroyProcess();
    }

    @Override
    public synchronized void timeoutOccured(Watchdog w) {
        status.compareAndSet(INIT , TIMEOUT);

        try {
            if (Objects.nonNull(monitoringProcess)){
                monitoringProcess.exitValue();
            }
        }catch (final IllegalThreadStateException e){
            if (isWatching()){
                hasKilledProcess = true;
                if (hasValidPid()){
                    ScriptPidUtils.killAllChilewnByPid(pid , false);
                }
            }

        }

        super.timeoutOccured(w);
    }

    @Override
    public synchronized boolean killedProcess() {
        return hasKilledProcess;
    }

    @Override
    protected synchronized void cleanUp() {
        super.cleanUp();
        monitoringProcess = null;
    }

    private boolean hasValidPid() {
        return pid != INVALID_PID;
    }

    public boolean isTimeout() {
        return status.get() == TIMEOUT;
    }

    public boolean isForceStop() {
        return status.get() == FORCE_STOP;
    }

    /**
     * 获取处理进程的pid
     */
    public static long getPidByProcess(Process process) {
        // linux ,unix ,mac os 应该

        if (!OS.isFamilyUnix()){
            return INVALID_PID;
        }
        try {
            String clsName = "java.lang.UNIXProcess";
            Class<?> cls = Class.forName(clsName);
            Field field = cls.getDeclaredField("pid");
            field.setAccessible(true);
            Object pid = field.get(process);
            LOGGER.debug( "Get Process Id: {}", pid);
            return Long.parseLong(pid.toString());
        }catch (Exception e){
            LOGGER.error("Getting pid error: {}", e.getMessage(), e);
            return INVALID_PID;
        }

    }
}
