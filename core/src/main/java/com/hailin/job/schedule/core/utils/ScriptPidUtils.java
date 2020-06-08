package com.hailin.job.schedule.core.utils;

import com.hailin.job.schedule.core.basic.storage.JobNodePath;
import com.hailin.job.schedule.core.reg.base.CoordinatorRegistryCenter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * 用于处理shell的相关pid功能
 */
public class ScriptPidUtils {

    private static final Logger log = LoggerFactory.getLogger(ScriptPidUtils.class);

    public static final long UNKNOWN_PID = -1;

    /**
     * 系统分隔符
     */
    protected static final String FILESEPARATOR = System.getProperty("file.separator");

    /**
     * Saturn的运行目录 <p> ${HOME}/.schedule/executing
     */
    public static final String EXECUTINGPATH =
            System.getProperty("user.home") + FILESEPARATOR + ".schedule" + FILESEPARATOR + "executing";

    /**
     * Saturn的运行目录 <p> ${HOME}/.schedule/output
     */
    public static final String OUTPUT_PATH =
            System.getProperty("user.home") + FILESEPARATOR + ".schedule" + FILESEPARATOR + "output";

    /**
     * 作业执行的运行目录 <p> 目录: ${HOME}/.schedule/executing/[executorName]/[jobName]
     */
    public static final String EXECUTINGJOBPATH = EXECUTINGPATH + FILESEPARATOR + "%s" + FILESEPARATOR + "%s";

    /**
     * 作业执行的Pid文件 <p> 目录: ${HOME}/.schedule/executing/[executorName]/[jobName]/[jobItem]/PID
     */
    public static final String JOBITEMPIDSPATH = EXECUTINGJOBPATH + FILESEPARATOR + "%s" + FILESEPARATOR + "PIDS";
    public static final String JOBITEMPATH = EXECUTINGJOBPATH + FILESEPARATOR + "%s";

    public static final String JOBITEMPIDPATH2 =
            EXECUTINGJOBPATH + FILESEPARATOR + "%s" + FILESEPARATOR + "PIDS" + FILESEPARATOR + "%s";

    /**
     * Shell作业执行的回写结果路径文件 <p> 目录: ${HOME}/.schedule/output/[executorName]/[jobName]/[jobItem]/[randomId/messageId
     * ]/[timestamp]
     */
    public static final String JOBITEMOUTPUTPATH =
            OUTPUT_PATH + FILESEPARATOR + "%s" + FILESEPARATOR + "%s" + FILESEPARATOR + "%s" + FILESEPARATOR + "%s"
                    + FILESEPARATOR + "%s";

    private static final String CHECK_RUNNING_JOB_THREAD_NAME = "check-if-job-%s-done";

    public static void checkAllExistJobs(final CoordinatorRegistryCenter regCenter) {
        List<String> zkJobNames = regCenter.getChildrenKeys(JobNodePath.ROOT);
        if (CollectionUtils.isEmpty(zkJobNames)){
            return;
        }
        for (final String jobName : zkJobNames  ) {
            checkOneExistJob(regCenter , jobName);
        }
    }

    private static void checkOneExistJob(CoordinatorRegistryCenter regCenter, String jobName) {
    }
}
