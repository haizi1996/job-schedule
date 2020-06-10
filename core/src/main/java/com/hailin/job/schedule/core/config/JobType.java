package com.hailin.job.schedule.core.config;


import com.hailin.job.schedule.core.basic.AbstractElasticJob;
import com.hailin.job.schedule.core.job.trigger.Trigger;

/**
 * 作业类型
 */
public interface JobType {

    Class<? extends AbstractElasticJob> getHandlerClass() ;

    Class<? extends Trigger> getTriggerClass();
}
