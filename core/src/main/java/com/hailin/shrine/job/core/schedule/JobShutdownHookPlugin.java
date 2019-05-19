package com.hailin.shrine.job.core.schedule;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.plugins.management.ShutdownHookPlugin;
import org.quartz.spi.ClassLoadHelper;

/**
 * 作业关闭的钩子
 * @author zhanghailin
 */
public class JobShutdownHookPlugin extends ShutdownHookPlugin {

    private String jobName;

    @Override
    public void initialize(String name, Scheduler scheduler, ClassLoadHelper classLoadHelper) throws SchedulerException {
        super.initialize(name, scheduler, classLoadHelper);
        jobName = scheduler.getSchedulerName();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }
}
