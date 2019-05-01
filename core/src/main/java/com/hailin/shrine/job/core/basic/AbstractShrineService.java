package com.hailin.shrine.job.core.basic;

import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;

/**
 * 抽象服务类
 * @author zhanghailin
 */
public class AbstractShrineService implements AutoCloseable {

    //执行者名字
    protected String executorName;

    //作业名称
    protected String jobName;

    //注册中心
    protected CoordinatorRegistryCenter coordinatorRegistryCenter;

    protected

    @Override
    public void close() throws Exception {

    }
}
