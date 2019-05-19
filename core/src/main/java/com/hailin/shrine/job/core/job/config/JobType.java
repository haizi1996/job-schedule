package com.hailin.shrine.job.core.job.config;

import com.hailin.shrine.job.core.basic.AbstractElasticJob;
import com.hailin.shrine.job.core.job.trigger.Trigger;

/**
 * 作业类型
 */
public interface JobType {
    String getName();

    Class<? extends Trigger> getTriggerClass();

    Class<? extends AbstractElasticJob> getHandlerClass();

    boolean isCron();

    boolean isPassive();

    boolean isJava();

    boolean isShell();

    boolean isAllowedShutdownGracefully();
}
