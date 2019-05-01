package com.hailin.hjob.common.event;

import com.hailin.hjob.common.context.ExecutionType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.UUID;

/**
 * 作业状态追踪事件.
 * @author zhanghailin
 */
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public class JobStatusTraceEvent implements JobEvent{

    private String id = UUID.randomUUID().toString();

    private final String jobName;

    @Setter
    private String originalTaskId = "";

    private final String taskId;

    private final String slaveId;

    private final Source source;

    private final ExecutionType executionType;

    private final String shardingItems;

    private final State state;

    private final String message;

    private Date creationTime = new Date();

    public enum State {
        TASK_STAGING, TASK_RUNNING, TASK_FINISHED, TASK_KILLED, TASK_LOST, TASK_FAILED, TASK_ERROR, TASK_DROPPED, TASK_GONE, TASK_GONE_BY_OPERATOR, TASK_UNREACHABLE, TASK_UNKNOWN
    }

    public enum Source {
        CLOUD_SCHEDULER, CLOUD_EXECUTOR, LITE_EXECUTOR
    }

}
