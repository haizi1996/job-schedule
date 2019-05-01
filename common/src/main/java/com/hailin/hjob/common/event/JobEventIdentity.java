package com.hailin.hjob.common.event;

/**
 * 作业事件标识
 * @author zhanghailin
 */
public interface JobEventIdentity {

    /**
     * 获取作业事件标识.
     *
     * @return 作业事件标识
     */
    String getIdentity();
}
