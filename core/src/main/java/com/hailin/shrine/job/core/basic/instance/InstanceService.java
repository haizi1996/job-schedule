package com.hailin.shrine.job.core.basic.instance;

import com.hailin.shrine.job.core.basic.server.ServerService;
import com.hailin.shrine.job.core.basic.storage.JobNodeStorage;
import com.hailin.shrine.job.core.reg.base.CoordinatorRegistryCenter;

/**
 * 作业运行实例服务
 */
public final class InstanceService {

    private JobNodeStorage jobNodeStorage;

    private ServerService serverService;

    private InstanceNode instanceNode;

    public InstanceService(CoordinatorRegistryCenter registryCenter , String jobName) {
        serverService = new ServerService();
    }
}
