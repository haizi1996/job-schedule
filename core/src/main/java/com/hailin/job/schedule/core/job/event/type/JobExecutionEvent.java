
package com.hailin.job.schedule.core.job.event.type;

import com.hailin.shrine.job.common.event.JobExecutionEventThrowable;
import com.hailin.shrine.job.common.util.ExceptionUtil;
import com.hailin.shrine.job.common.util.IpUtils;
import com.hailin.job.schedule.core.job.event.JobEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.UUID;

/**
 * 作业执行事件.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public final class JobExecutionEvent implements JobEvent {
    
    private String id = UUID.randomUUID().toString();
    
    private String hostname = IpUtils.getHostName();
    
    private String ip = IpUtils.getIp();
    
    private final String taskId;
    
    private final String jobName;
    
    private final ExecutionSource source;
    
    private final int shardingItem;
    
    private Date startTime = new Date();
    
    @Setter
    private Date completeTime;
    
    @Setter
    private boolean success;
    
    @Setter
    private JobExecutionEventThrowable failureCause;
    
    /**
     * 作业执行成功.
     * 
     * @return 作业执行事件
     */
    public JobExecutionEvent executionSuccess() {
        JobExecutionEvent result = new JobExecutionEvent(id, hostname, ip, taskId, jobName, source, shardingItem, startTime, completeTime, success, failureCause);
        result.setCompleteTime(new Date());
        result.setSuccess(true);
        return result;
    }
    
    /**
     * 作业执行失败.
     * 
     * @param failureCause 失败原因
     * @return 作业执行事件
     */
    public JobExecutionEvent executionFailure(final Throwable failureCause) {
        JobExecutionEvent result = new JobExecutionEvent(id, hostname, ip, taskId, jobName, source, shardingItem, startTime, completeTime, success, new JobExecutionEventThrowable(failureCause));
        result.setCompleteTime(new Date());
        result.setSuccess(false);
        return result;
    }
    
    /**
     * 获取失败原因.
     * 
     * @return 失败原因
     */
    public String getFailureCause() {
        return ExceptionUtil.transform(failureCause == null ? null : failureCause.getThrowable());
    }
    
    /**
     * 执行来源.
     */
    public enum ExecutionSource {
        NORMAL_TRIGGER, MISFIRE, FAILOVER
    }
}
