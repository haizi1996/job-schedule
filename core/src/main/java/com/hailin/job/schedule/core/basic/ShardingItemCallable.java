package com.hailin.job.schedule.core.basic;

import com.hailin.shrine.job.ScheduleJobReturn;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * 分片上下文定义类
 *
 */
@Getter
@Setter
@RequiredArgsConstructor
public class ShardingItemCallable {

    protected final String jobName;

    protected final Integer item;

    protected final String itemValue;

    protected final int timeoutSeconds; // second

    protected final ScheduleExecutionContext shardingContext;

    protected final AbstractScheduleJob scheduleJob;

    protected ScheduleJobReturn scheduleJobReturn;

    protected Map<String, String> envMap = new HashMap<>();

    protected boolean businessReturned = false;

    protected long startTime;

    protected long endTime;
}
