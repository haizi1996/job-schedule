package com.hailin.job.schedule.core.basic;

/**
 * 可以停止的任务
 * @author zhanghailin
 */
public interface Stoppable {

    /**
     * 停止运行中的作业
     */
    void stop();

    /**
     * 中止作业
     * 上报状态
     */
    void forceStop();

    /**
     *  恢复运行作业
     */
    void resume();

    /**
     * 中止状态
     * 不上报状态
     */
    void abort();

    /**
     *  关闭作业
     */
    void shutdown();
}
