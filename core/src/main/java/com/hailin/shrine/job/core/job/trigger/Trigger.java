package com.hailin.shrine.job.core.job.trigger;

import com.hailin.shrine.job.core.basic.AbstractElasticJob;

/**
 * 定时任务触发接口
 * @author zhanghailin
 */
public interface Trigger {

    /**
     * 初始化
     */
    void init(AbstractElasticJob job);

    /**
     *  新建一个quartz的触发器类
     */
    org.quartz.Trigger createQuartzTrigger();

    boolean isInitialTriggered();

    void enabledJob();

    void disableJob();

    void onReSharding();

    boolean isFailoverSupported();

    Triggered createTriggered(boolean yes, String upStreamDataStr);

    String serializeDownStreamData(Triggered triggered);
}
