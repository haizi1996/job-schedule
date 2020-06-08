package com.hailin.job.schedule.core.basic.storage;

/**
 * 主节点执行操作的回调接口
 * @author zhanghailin
 */
public interface LeaderExecutionCallback {

    /**
     * 节点选中之后执行的回调操作
     */
    void execute();
}
