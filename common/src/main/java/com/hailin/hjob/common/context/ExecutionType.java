package com.hailin.hjob.common.context;

/**
 * 执行类型
 * @author zhanghailin
 */
public enum  ExecutionType {

    /**
     * 准备执行的任务.
     */
    READY,

    /**
     * 失效转移的任务.
     */
    FAILOVER
}
