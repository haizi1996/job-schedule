package com.hailin.job.schedule.core.strategy;

import com.hailin.shrine.job.common.util.IpUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.management.ManagementFactory;

/**
 * 任务运行实例.
 * @author zhanghailin
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode(of = "jobInstanceId")
public final class JobInstance {

    private static final String DELIMITER = "@-@";

    /**
     * 作业实例主键.
     */
    private final String jobInstanceId;

    public JobInstance() {
        jobInstanceId = IpUtils.getIp() + DELIMITER + ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }

    /**
     * 获取作业服务器IP地址.
     *
     * @return 作业服务器IP地址
     */
    public String getIp() {
        return jobInstanceId.substring(0, jobInstanceId.indexOf(DELIMITER));
    }

}
